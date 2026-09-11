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
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// cluster is the harness's view of the load cluster: a typed client for the
// objects it creates and watches, a clientset for logs and raw API paths, and
// the context name every kubectl shell-out pins to, so a stray current-context
// can never point the run at the shared e2e cluster.
type cluster struct {
	context string
	cfg     *rest.Config
	c       client.Client
	cs      *kubernetes.Clientset
}

func connect(kubeContext string) (*cluster, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules,
		&clientcmd.ConfigOverrides{CurrentContext: kubeContext}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig context %q: %w", kubeContext, err)
	}
	// The ramp creates hundreds of objects in bursts; the client-go defaults
	// (5 QPS) would make the harness, not the operator, the thing under test.
	cfg.QPS = 200
	cfg.Burst = 400

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		kaalmv1beta1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme, cmapi.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, err
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &cluster{context: kubeContext, cfg: cfg, c: c, cs: cs}, nil
}

// kubectl runs kubectl pinned to the load cluster's context; a failure
// carries the command's output.
func (k *cluster) kubectl(args ...string) error {
	full := append([]string{"--context", k.context}, args...)
	out, err := exec.Command("kubectl", full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, string(out))
	}
	return nil
}

// rawGet fetches an absolute API path (the metrics.k8s.io endpoints have no
// typed client in the module).
func (k *cluster) rawGet(ctx context.Context, absPath string) ([]byte, error) {
	return k.cs.Discovery().RESTClient().Get().AbsPath(absPath).DoRaw(ctx)
}

// portForward starts `kubectl port-forward` to a pod or service and returns
// the kubectl-chosen local port. The caller must call stop.
func (k *cluster) portForward(namespace, target, remotePort string) (int, func(), error) {
	cmd := exec.Command("kubectl", "--context", k.context, "port-forward", "-n", namespace, target, ":"+remotePort)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	stop := func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }

	portCh := make(chan int, 1)
	go func() {
		const marker = "Forwarding from 127.0.0.1:"
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			i := strings.Index(line, marker)
			if i < 0 {
				continue
			}
			rest := line[i+len(marker):]
			if j := strings.IndexByte(rest, ' '); j > 0 {
				if p, err := strconv.Atoi(rest[:j]); err == nil {
					portCh <- p
					// Keep draining so a long-lived forward never blocks on a
					// full pipe; the pipe closes when stop kills kubectl.
					_, _ = io.Copy(io.Discard, stdout)
					return
				}
			}
		}
		portCh <- 0
	}()
	select {
	case p := <-portCh:
		if p == 0 {
			stop()
			return 0, nil, fmt.Errorf("port-forward to %s did not report a local port", target)
		}
		return p, stop, nil
	case <-time.After(20 * time.Second):
		stop()
		return 0, nil, fmt.Errorf("port-forward to %s timed out", target)
	}
}

// usage is one component's resource consumption from the metrics API.
type usage struct {
	CPUMilli float64 `json:"cpuMilli"`
	MemMiB   float64 `json:"memMiB"`
}

// podUsage sums metrics.k8s.io pod usage per name prefix (the Deployment
// name), so "kaalm-gateway" is the sum over every gateway replica.
func (k *cluster) podUsage(ctx context.Context, namespace string) (map[string]usage, error) {
	raw, err := k.rawGet(ctx, "/apis/metrics.k8s.io/v1beta1/namespaces/"+namespace+"/pods")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata   struct{ Name string }
			Containers []struct {
				Usage map[string]string
			}
		}
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	out := map[string]usage{}
	for _, item := range list.Items {
		prefix := deploymentPrefix(item.Metadata.Name)
		u := out[prefix]
		for _, c := range item.Containers {
			u.CPUMilli += quantityFloat(c.Usage["cpu"]) * 1000
			u.MemMiB += quantityFloat(c.Usage["memory"]) / (1 << 20)
		}
		out[prefix] = u
	}
	return out, nil
}

// nodeUsage returns per-node memory working set in MiB from the metrics API.
func (k *cluster) nodeUsage(ctx context.Context) (map[string]usage, error) {
	raw, err := k.rawGet(ctx, "/apis/metrics.k8s.io/v1beta1/nodes")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata struct{ Name string }
			Usage    map[string]string
		}
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	out := map[string]usage{}
	for _, item := range list.Items {
		out[item.Metadata.Name] = usage{
			CPUMilli: quantityFloat(item.Usage["cpu"]) * 1000,
			MemMiB:   quantityFloat(item.Usage["memory"]) / (1 << 20),
		}
	}
	return out, nil
}

// deploymentPrefix strips the ReplicaSet and Pod hash suffixes from a
// Deployment-owned Pod name: kaalm-gateway-7b4b478585-k9qmd -> kaalm-gateway.
func deploymentPrefix(pod string) string {
	parts := strings.Split(pod, "-")
	if len(parts) > 2 {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return pod
}

func quantityFloat(s string) float64 {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return q.AsApproximateFloat64()
}

// hostMemAvailableMiB reads the kernel's MemAvailable. Under WSL2 the k3d
// node containers share this kernel, so it is the number that actually bounds
// the ramp.
func hostMemAvailableMiB() (float64, error) {
	return meminfoMiB("MemAvailable:")
}

func hostMemTotalMiB() (float64, error) {
	return meminfoMiB("MemTotal:")
}

func meminfoMiB(key string) (float64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, key) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return 0, err
		}
		return kb / 1024, nil
	}
	return 0, fmt.Errorf("%s not found in /proc/meminfo", key)
}
