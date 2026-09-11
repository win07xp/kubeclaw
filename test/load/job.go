//go:build loadtest

/*
Copyright 2026 The Kaalm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// runLoadgen runs the in-cluster load generator as a Job, waits for it, and
// parses the RESULT_JSON line from its log. The Job mounts what a workload
// at the gateway-only tier has: a ServiceAccount token projected for the
// kaalm-gateway audience, the kaalm-ca bundle, and (for channel traffic) the
// webhook bearer secret. A non-empty mtlsSecret additionally mounts that
// TLS Secret at /var/run/mtls, the identity the mTLS legs present.
func (k *cluster) runLoadgen(
	ctx context.Context, ns, name, image string, args []string, mtlsSecret string, timeout time.Duration,
) (*loadResult, error) {
	// A previous run's Job of the same name must not confuse the log lookup.
	stale := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	_ = k.c.Delete(ctx, stale, client.PropagationPolicy(metav1.DeletePropagationForeground))
	_ = pollUntil(ctx, time.Minute, 2*time.Second, func() (bool, error) {
		err := k.c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &batchv1.Job{})
		return apierrors.IsNotFound(err), nil
	})

	backoff := int32(0)
	deadline := int64(timeout.Seconds())
	tokenExpiry := int64(3600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{phaseLabel: loadgenName}},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoff,
			ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "loadgen"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: loadgenName,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            loadgenName,
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            append([]string{loadgenName}, args...),
						VolumeMounts: []corev1.VolumeMount{
							{Name: tokenVolume, MountPath: "/var/run/token", ReadOnly: true},
							{Name: "ca", MountPath: "/var/run/ca", ReadOnly: true},
							{Name: "hook", MountPath: "/var/run/hook", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: tokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
								Audience: "kaalm-gateway", ExpirationSeconds: &tokenExpiry, Path: tokenVolume,
							}}},
						}}},
						{Name: "ca", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "kaalm-ca"},
							Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
						}}},
						{Name: "hook", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
							SecretName: hookSecretName,
						}}},
					},
				},
			},
		},
	}
	if mtlsSecret != "" {
		pod := &job.Spec.Template.Spec
		pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "mtls", MountPath: "/var/run/mtls", ReadOnly: true})
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "mtls", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: mtlsSecret},
		}})
	}
	if err := k.c.Create(ctx, job); err != nil {
		return nil, fmt.Errorf("create loadgen job: %w", err)
	}
	defer func() {
		_ = k.c.Delete(context.Background(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	}()

	var failed bool
	err := pollUntil(ctx, timeout+2*time.Minute, 5*time.Second, func() (bool, error) {
		var cur batchv1.Job
		if err := k.c.Get(ctx, client.ObjectKeyFromObject(job), &cur); err != nil {
			return false, err
		}
		if cur.Status.Succeeded > 0 {
			return true, nil
		}
		if cur.Status.Failed > 0 {
			failed = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("loadgen job %s: %w", name, err)
	}
	logs, logErr := k.jobLogs(ctx, ns, name)
	if failed {
		return nil, fmt.Errorf("loadgen job %s failed: %s", name, lastLines(logs, 10))
	}
	if logErr != nil {
		return nil, logErr
	}
	for _, line := range strings.Split(logs, "\n") {
		if strings.HasPrefix(line, resultMarker) {
			var res loadResult
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, resultMarker)), &res); err != nil {
				return nil, fmt.Errorf("parse loadgen result: %w", err)
			}
			return &res, nil
		}
	}
	return nil, fmt.Errorf("loadgen job %s printed no result: %s", name, lastLines(logs, 10))
}

func (k *cluster) jobLogs(ctx context.Context, ns, job string) (string, error) {
	var pods corev1.PodList
	if err := k.c.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{"job-name": job}); err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pod for job %s", job)
	}
	stream, err := k.cs.CoreV1().Pods(ns).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = stream.Close() }()
	var b strings.Builder
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		b.WriteString(scanner.Text())
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
