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

package controller

import (
	"context"
	"fmt"
	"time"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// provisioningDeadline bounds how long an image-pull or scheduling failure may
// persist before the task fails. A documented constant, not a spec field
// (docs/src/controller/task-lifecycle.md). A variable so tests can shorten it.
var provisioningDeadline = 5 * time.Minute

// AgentTaskReconciler drives the run-to-completion state machine: Pending ->
// Provisioning -> Running -> Completing -> Succeeded/Failed/TimedOut, with
// backoffLimit retries bracketed by the currentPodUID identity gate and the
// completion mailbox. See docs/src/controller/reconcilers.md
// (AgentTaskReconciler) and task-lifecycle.md.
type AgentTaskReconciler struct {
	client.Client
	Recorder          record.EventRecorder
	OperatorNamespace string
	// MaxConcurrentReconciles is the number of reconciles that may run at
	// once; controller-runtime still serializes per object. 0 means one.
	MaxConcurrentReconciles int
}

// +kubebuilder:rbac:groups=kaalm.io,resources=agenttasks,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=kaalm.io,resources=agenttasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kaalm.io,resources=agenttasks/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

// Reconcile runs one pass of the AgentTask state machine.
func (r *AgentTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var task kaalmv1beta1.AgentTask
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !task.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &task)
	}
	if controllerutil.AddFinalizer(&task, kaalmv1beta1.TaskFinalizer) {
		if err := r.Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	task.Status.ObservedGeneration = task.Generation
	if task.Status.Phase == "" {
		task.Status.Phase = kaalmv1beta1.TaskPending
	}

	// A Failed phase with no completionTime is a crash-interrupted retry (the
	// retry sequence writes Failed, then Provisioning): resume it. A Failed
	// phase with completionTime set is terminal.
	if task.Status.Phase == kaalmv1beta1.TaskFailed && task.Status.CompletionTime == nil {
		r.setTaskPhase(&task, kaalmv1beta1.TaskProvisioning)
		if err := r.Status().Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal phases only wait out their TTL.
	if isTerminalTaskPhase(task.Status.Phase) {
		return r.handleTTL(ctx, &task)
	}

	// System-namespace guard (same SAN-integrity rule as Agents).
	if task.Namespace == r.OperatorNamespace {
		r.setTaskReady(&task, false, kaalmv1beta1.ReasonSystemNamespaceForbidden,
			fmt.Sprintf("AgentTasks may not run in the operator namespace %q", r.OperatorNamespace))
		return ctrl.Result{}, r.Status().Update(ctx, &task)
	}

	var class kaalmv1beta1.AgentClass
	if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.AgentClassRef.Name}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			r.setTaskReady(&task, false, kaalmv1beta1.ReasonInvalidReference,
				fmt.Sprintf("AgentClass %q does not exist", task.Spec.AgentClassRef.Name))
			return ctrl.Result{}, r.Status().Update(ctx, &task)
		}
		return ctrl.Result{}, err
	}
	eff := deriveEffectiveTaskSpec(&task, &class)

	pod, err := r.ownedTaskPod(ctx, &task)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Pre-Pod validation runs only when provisioning a new Pod (initial
	// provisioning and the Provisioning re-entry of a backoff retry).
	// In-flight tasks continue under the class snapshot taken at Pod creation.
	if pod == nil && (task.Status.Phase == kaalmv1beta1.TaskPending ||
		task.Status.Phase == kaalmv1beta1.TaskProvisioning) {
		if reason, msg := r.taskViolation(ctx, &task, &class, eff); reason != "" {
			// Terminal: AgentTask has no Degraded phase.
			return ctrl.Result{}, r.settle(ctx, &task, kaalmv1beta1.TaskFailed, reason, msg)
		}
		for _, ref := range eff.ImagePullSecrets {
			var sec corev1.Secret
			err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: ref.Name}, &sec)
			if apierrors.IsNotFound(err) {
				r.setTaskReady(&task, false, kaalmv1beta1.ReasonImagePullSecretMissing,
					fmt.Sprintf("imagePullSecret %q missing in namespace %q", ref.Name, task.Namespace))
				if err := r.Status().Update(ctx, &task); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: gateRequeue}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}
		}
		if eff.Image == "" {
			r.setTaskReady(&task, false, kaalmv1beta1.ReasonInvalidReference,
				"no image: AgentTask.spec.image is empty and the AgentClass sets no defaultImage")
			return ctrl.Result{}, r.Status().Update(ctx, &task)
		}
	}

	// A Failed task reaching this point is in the retry path: the eligibility
	// was decided when Failed was entered (settle vs retry), so a lingering
	// Failed phase with a live retry has already transitioned to Provisioning.
	res, err := r.drive(ctx, &task, &class, eff, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	logger.V(1).Info("reconciled AgentTask", "phase", task.Status.Phase)
	return res, nil
}

// drive advances the non-terminal state machine given the current Pod.
func (r *AgentTaskReconciler) drive(
	ctx context.Context, task *kaalmv1beta1.AgentTask, class *kaalmv1beta1.AgentClass,
	eff effectiveTaskSpec, pod *corev1.Pod,
) (ctrl.Result, error) {
	switch task.Status.Phase {
	case kaalmv1beta1.TaskPending, kaalmv1beta1.TaskProvisioning:
		return r.driveProvisioning(ctx, task, class, eff, pod)
	case kaalmv1beta1.TaskRunning:
		return r.driveRunning(ctx, task, pod)
	case kaalmv1beta1.TaskCompleting:
		return ctrl.Result{}, r.driveCompleting(ctx, task, pod)
	}
	return ctrl.Result{}, nil
}

// driveProvisioning creates the child tree, gates on the Certificate, creates
// the Pod, and watches it to Ready or an early failure.
func (r *AgentTaskReconciler) driveProvisioning(
	ctx context.Context, task *kaalmv1beta1.AgentTask, class *kaalmv1beta1.AgentClass,
	eff effectiveTaskSpec, pod *corev1.Pod,
) (ctrl.Result, error) {
	if pod == nil {
		certReady, err := r.ensureTaskCertificate(ctx, task)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !certReady {
			if task.Status.Phase == kaalmv1beta1.TaskPending {
				r.setTaskPhase(task, kaalmv1beta1.TaskProvisioning)
			}
			r.setTaskReady(task, false, "CertificateNotReady", "waiting for cert-manager to issue the task certificate")
			if err := r.Status().Update(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: certWaitRequeue}, nil
		}
		if err := r.ensureTaskChildren(ctx, task, class, eff); err != nil {
			return ctrl.Result{}, err
		}
		desired := desiredTaskPod(task, eff, r.OperatorNamespace)
		if err := controllerutil.SetControllerReference(task, desired, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, err
		}
		r.setTaskPhase(task, kaalmv1beta1.TaskProvisioning)
		r.setTaskReady(task, false, "PodProvisioning", "task Pod created, waiting for readiness")
		task.Status.PodName = desired.Name
		if isAgentReported(task) {
			task.Status.CurrentPodUID = string(desired.UID)
		}
		return ctrl.Result{}, r.Status().Update(ctx, task)
	}

	// Stamp identity on the observed Pod (re-opens the gate after a retry).
	if isAgentReported(task) && task.Status.CurrentPodUID != string(pod.UID) {
		task.Status.CurrentPodUID = string(pod.UID)
		task.Status.PodName = pod.Name
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}

	if podReady(pod) {
		r.setTaskPhase(task, kaalmv1beta1.TaskRunning)
		now := metav1.Now()
		task.Status.StartTime = &now
		r.setTaskReady(task, true, "PodRunning", "task Pod is running")
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		return r.runningRequeue(task), nil
	}

	// Terminal before Ready: under restartPolicy Never one crash is final.
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		// An exitCode task whose container ran to completion before the Ready
		// condition ever flipped is still a completion, not a provisioning
		// failure: fall through to Completing.
		if !isAgentReported(task) && pod.Status.Phase == corev1.PodSucceeded {
			r.setTaskPhase(task, kaalmv1beta1.TaskCompleting)
			if err := r.Status().Update(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.driveCompleting(ctx, task, pod)
		}
		return ctrl.Result{}, r.failOrRetry(ctx, task, "PodStartFailed",
			fmt.Sprintf("Pod %s reached %s before becoming Ready", pod.Name, pod.Status.Phase))
	}

	// Fatal config errors fail immediately; pull/scheduling failures fail
	// after the provisioning deadline.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		switch cs.State.Waiting.Reason {
		case "InvalidImageName", "ErrImageNeverPull":
			return ctrl.Result{}, r.failOrRetry(ctx, task, cs.State.Waiting.Reason, cs.State.Waiting.Message)
		}
	}
	if time.Since(pod.CreationTimestamp.Time) > provisioningDeadline {
		return ctrl.Result{}, r.failOrRetry(ctx, task, "ProvisioningDeadlineExceeded",
			fmt.Sprintf("Pod %s not Ready within %s", pod.Name, provisioningDeadline))
	}
	return ctrl.Result{RequeueAfter: certWaitRequeue}, r.Status().Update(ctx, task)
}

// driveRunning watches for completion, timeout, and mid-run Pod loss, in that
// precedence order: a completion already in the mailbox beats a lost Pod.
func (r *AgentTaskReconciler) driveRunning(
	ctx context.Context, task *kaalmv1beta1.AgentTask, pod *corev1.Pod,
) (ctrl.Result, error) {
	// agentReported: the mailbox is the completion signal.
	if isAgentReported(task) {
		payload, err := r.readMailbox(ctx, task)
		if err != nil {
			return ctrl.Result{}, err
		}
		if payload.Status != "" {
			r.setTaskPhase(task, kaalmv1beta1.TaskCompleting)
			if err := r.Status().Update(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.driveCompleting(ctx, task, pod)
		}
	}

	// exitCode: a terminal Pod is the completion signal.
	if !isAgentReported(task) && pod != nil &&
		(pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) {
		r.setTaskPhase(task, kaalmv1beta1.TaskCompleting)
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.driveCompleting(ctx, task, pod)
	}

	// Timeout: measured from startTime, so scheduling never counts.
	if timedOut(task) {
		r.setTaskPhase(task, kaalmv1beta1.TaskCompleting)
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.driveCompleting(ctx, task, pod)
	}

	// Mid-run Pod loss with an empty mailbox is a retryable failure.
	if pod == nil || (isAgentReported(task) && pod.Status.Phase == corev1.PodFailed) {
		return ctrl.Result{}, r.failOrRetry(ctx, task, "PodDisrupted", "task Pod was lost mid-run")
	}
	return r.runningRequeue(task), nil
}

// driveCompleting settles the terminal phase. The outcome is re-derived from
// the mailbox, the Pod, and the clock rather than stored: mailbox payload
// first, then container exit, then timeout.
func (r *AgentTaskReconciler) driveCompleting(
	ctx context.Context, task *kaalmv1beta1.AgentTask, pod *corev1.Pod,
) error {
	if isAgentReported(task) {
		payload, err := r.readMailbox(ctx, task)
		if err != nil {
			return err
		}
		if payload.Status != "" {
			if msg := validateArtifactNames(payload, task.Spec.Artifacts); msg != "" {
				// The gateway enforces the same rule synchronously, so this
				// firing means something drifted.
				return r.failOrRetry(ctx, task, kaalmv1beta1.ReasonTaskFailed, "artifact validation failed: "+msg)
			}
			task.Status.ArtifactValues = payload.Artifacts
			task.Status.AgentReportedStatus = payload.Status
			task.Status.AgentReportedMessage = payload.Message
			if payload.Status == completionStatusSuccess {
				return r.settle(ctx, task, kaalmv1beta1.TaskSucceeded,
					kaalmv1beta1.ReasonTaskSucceeded, payload.Message)
			}
			return r.failOrRetry(ctx, task, kaalmv1beta1.ReasonTaskFailed,
				"agent reported failure: "+payload.Message)
		}
		// No payload: this Completing pass was timeout-triggered.
		if timedOut(task) {
			return r.settleTimeout(ctx, task)
		}
		return r.failOrRetry(ctx, task, "PodDisrupted", "task reached Completing with no completion payload")
	}

	// exitCode mode.
	if pod != nil && pod.Status.Phase == corev1.PodSucceeded {
		return r.settle(ctx, task, kaalmv1beta1.TaskSucceeded,
			kaalmv1beta1.ReasonTaskSucceeded, "container exited 0")
	}
	if pod != nil && pod.Status.Phase == corev1.PodFailed {
		return r.failOrRetry(ctx, task, kaalmv1beta1.ReasonTaskFailed, podExitMessage(pod))
	}
	if timedOut(task) {
		return r.settleTimeout(ctx, task)
	}
	// Pod vanished between Running and Completing.
	return r.failOrRetry(ctx, task, "PodDisrupted", "task Pod was lost before completion settled")
}

// settleTimeout applies spec.completion.onTimeout: Fail (default) settles
// TimedOut, Succeed settles Succeeded. TimedOut is exempt from backoffLimit.
func (r *AgentTaskReconciler) settleTimeout(ctx context.Context, task *kaalmv1beta1.AgentTask) error {
	if task.Spec.Completion.OnTimeout == onTimeoutSucceed {
		return r.settle(ctx, task, kaalmv1beta1.TaskSucceeded, "TimeoutSucceeded",
			"timeout reached with onTimeout: Succeed")
	}
	return r.settle(ctx, task, kaalmv1beta1.TaskTimedOut, "TimeoutExceeded",
		fmt.Sprintf("task exceeded its %s completion timeout", task.Spec.Completion.Timeout.Duration))
}

// failOrRetry either executes the retry sequence (backoffLimit permitting) or
// settles the task in terminal Failed.
func (r *AgentTaskReconciler) failOrRetry(
	ctx context.Context, task *kaalmv1beta1.AgentTask, reason, msg string,
) error {
	if task.Status.Retries < task.Spec.Completion.BackoffLimit {
		return r.retry(ctx, task, reason, msg)
	}
	return r.settle(ctx, task, kaalmv1beta1.TaskFailed, reason, msg)
}

// retry runs the documented sequence in order: increment retries, clear the
// UID (gate closes), delete the old Pod, reset the mailbox, transition back to
// Provisioning. The new Pod's UID is stamped when the informer observes it.
// The clear-before-reset ordering is load-bearing: resetting the mailbox first
// would let an in-flight stale write land on the fresh mailbox.
func (r *AgentTaskReconciler) retry(ctx context.Context, task *kaalmv1beta1.AgentTask, reason, msg string) error {
	r.Recorder.Event(task, corev1.EventTypeWarning, reason,
		fmt.Sprintf("%s; retrying (%d/%d)", msg, task.Status.Retries+1, task.Spec.Completion.BackoffLimit))

	// Steps 1 and 2 in one status write: the counter moves and the gate closes.
	task.Status.Retries++
	task.Status.CurrentPodUID = ""
	task.Status.StartTime = nil
	task.Status.ArtifactValues = nil
	task.Status.AgentReportedStatus = ""
	task.Status.AgentReportedMessage = ""
	r.setTaskPhase(task, kaalmv1beta1.TaskFailed)
	r.setTaskReady(task, false, reason, msg)
	if err := r.Status().Update(ctx, task); err != nil {
		return err
	}

	// Step 3: delete the old Pod if any remains.
	pod, err := r.ownedTaskPod(ctx, task)
	if err != nil {
		return err
	}
	if pod != nil && pod.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	// Step 4: reset the mailbox in place, preserving ownerRef and the scoped
	// Role's validity.
	if isAgentReported(task) {
		var cm corev1.ConfigMap
		key := types.NamespacedName{Namespace: task.Namespace, Name: taskCompletionCMName(task.Name)}
		if err := r.Get(ctx, key, &cm); err == nil {
			if len(cm.Data) > 0 {
				cm.Data = map[string]string{}
				if err := r.Update(ctx, &cm); err != nil {
					return err
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}

	// Step 6: back to Provisioning. Pod recreation happens on the next pass
	// once the old Pod is gone.
	r.setTaskPhase(task, kaalmv1beta1.TaskProvisioning)
	return r.Status().Update(ctx, task)
}

// settle commits a terminal phase with its condition and completion time.
func (r *AgentTaskReconciler) settle(
	ctx context.Context, task *kaalmv1beta1.AgentTask, phase kaalmv1beta1.AgentTaskPhase, reason, msg string,
) error {
	r.setTaskPhase(task, phase)
	now := metav1.Now()
	task.Status.CompletionTime = &now
	completed := metav1.ConditionFalse
	if phase == kaalmv1beta1.TaskSucceeded {
		completed = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: kaalmv1beta1.ConditionCompleted, Status: completed, Reason: reason, Message: msg,
	})
	r.setTaskReady(task, false, reason, msg)
	eventType := corev1.EventTypeNormal
	if phase != kaalmv1beta1.TaskSucceeded {
		eventType = corev1.EventTypeWarning
	}
	r.Recorder.Event(task, eventType, reason, msg)
	return r.Status().Update(ctx, task)
}

// handleTTL deletes a terminal task once ttlSecondsAfterFinished has elapsed.
func (r *AgentTaskReconciler) handleTTL(ctx context.Context, task *kaalmv1beta1.AgentTask) (ctrl.Result, error) {
	ttl := task.Spec.TTLSecondsAfterFinished
	if ttl == nil {
		return ctrl.Result{}, nil
	}
	finished := task.Status.CompletionTime
	if finished == nil {
		return ctrl.Result{}, nil
	}
	expiry := finished.Add(time.Duration(*ttl) * time.Second)
	if remaining := time.Until(expiry); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	r.setTaskPhase(task, kaalmv1beta1.TaskTerminating)
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, task))
}

// taskViolation runs the pre-Pod cross-checks. AgentTask has no Degraded
// phase, so any violation is a terminal Failed.
func (r *AgentTaskReconciler) taskViolation(
	ctx context.Context, task *kaalmv1beta1.AgentTask, class *kaalmv1beta1.AgentClass, eff effectiveTaskSpec,
) (string, string) {
	if eff.Image != "" && !imageAllowed(eff.Image, class.Spec.Image.AllowedImages) {
		return kaalmv1beta1.ReasonClassConstraintViolation,
			fmt.Sprintf("image %q does not match AgentClass %q allowedImages", eff.Image, class.Name)
	}
	for _, p := range task.Spec.Providers {
		name := p.ProviderRef.Name
		allowed := false
		for _, ap := range class.Spec.AllowedProviders {
			if ap.Name == name {
				allowed = true
				break
			}
		}
		if !allowed {
			return kaalmv1beta1.ReasonClassConstraintViolation,
				fmt.Sprintf("provider %q is not in AgentClass %q allowedProviders", name, class.Name)
		}
		var mp kaalmv1beta1.ModelProvider
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &mp); err != nil {
			if apierrors.IsNotFound(err) {
				return kaalmv1beta1.ReasonClassConstraintViolation,
					fmt.Sprintf("provider %q does not exist", name)
			}
			continue
		}
		if !namespaceAllowed(task.Namespace, mp.Spec.AllowedNamespaces) {
			return kaalmv1beta1.ReasonClassConstraintViolation,
				fmt.Sprintf("provider %q does not allow namespace %q", name, task.Namespace)
		}
	}
	// Rules 35 to 38 for the task's tool grants; the first violation wins,
	// terminal like every other task violation.
	if v := toolGrantViolations(ctx, r.Client, task.Namespace, task.Spec.Tools, class); len(v) > 0 {
		return v[0].Reason, v[0].Message
	}
	if task.Spec.Persistence.Enabled && !class.Spec.Persistence.Enabled {
		return kaalmv1beta1.ReasonPersistenceNotAllowed,
			fmt.Sprintf("persistence requested but AgentClass %q has persistence.enabled=false", class.Name)
	}
	return "", ""
}

// ensureTaskChildren converges the SA, PVC, NetworkPolicy, and, for
// agentReported tasks only, the completion mailbox with its scoped RBAC.
func (r *AgentTaskReconciler) ensureTaskChildren(
	ctx context.Context, task *kaalmv1beta1.AgentTask, class *kaalmv1beta1.AgentClass, eff effectiveTaskSpec,
) error {
	create := func(obj client.Object) error {
		if err := controllerutil.SetControllerReference(task, obj, r.Scheme()); err != nil {
			return err
		}
		err := r.Create(ctx, obj)
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	if err := create(desiredTaskServiceAccount(task)); err != nil {
		return err
	}
	if eff.PersistenceOn {
		if err := create(desiredTaskPVC(task, class, eff)); err != nil {
			return err
		}
	}
	if err := create(desiredTaskNetworkPolicy(task, class, r.OperatorNamespace)); err != nil {
		return err
	}
	if isAgentReported(task) {
		if err := create(desiredCompletionConfigMap(task)); err != nil {
			return err
		}
		if err := create(desiredCompletionRole(task)); err != nil {
			return err
		}
		if err := create(desiredCompletionRoleBinding(task, r.OperatorNamespace)); err != nil {
			return err
		}
	}
	return nil
}

func (r *AgentTaskReconciler) ensureTaskCertificate(ctx context.Context, task *kaalmv1beta1.AgentTask) (bool, error) {
	var cert cmapi.Certificate
	key := types.NamespacedName{Namespace: task.Namespace, Name: taskCertificateName(task.Name)}
	if err := r.Get(ctx, key, &cert); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		desired := desiredTaskCertificate(task)
		if err := controllerutil.SetControllerReference(task, desired, r.Scheme()); err != nil {
			return false, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return false, err
		}
		return false, nil
	}
	for _, c := range cert.Status.Conditions {
		if c.Type == cmapi.CertificateConditionReady && c.Status == cmmeta.ConditionTrue {
			return true, nil
		}
	}
	return false, nil
}

func (r *AgentTaskReconciler) readMailbox(ctx context.Context, task *kaalmv1beta1.AgentTask) (completionPayload, error) {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: task.Namespace, Name: taskCompletionCMName(task.Name)}
	if err := r.Get(ctx, key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return completionPayload{}, nil
		}
		return completionPayload{}, err
	}
	return parseCompletion(cm.Data), nil
}

func (r *AgentTaskReconciler) ownedTaskPod(ctx context.Context, task *kaalmv1beta1.AgentTask) (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(task.Namespace),
		client.MatchingLabels(taskPodLabels(task))); err != nil {
		return nil, err
	}
	var candidate *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if !metav1.IsControlledBy(p, task) {
			continue
		}
		if p.DeletionTimestamp.IsZero() {
			return p, nil
		}
		candidate = p
	}
	return candidate, nil
}

// runningRequeue schedules the next pass at the timeout deadline, when one is
// configured.
func (r *AgentTaskReconciler) runningRequeue(task *kaalmv1beta1.AgentTask) ctrl.Result {
	d := task.Spec.Completion.Timeout.Duration
	if d <= 0 || task.Status.StartTime == nil {
		return ctrl.Result{}
	}
	remaining := time.Until(task.Status.StartTime.Add(d))
	if remaining < time.Second {
		remaining = time.Second
	}
	return ctrl.Result{RequeueAfter: remaining}
}

func timedOut(task *kaalmv1beta1.AgentTask) bool {
	d := task.Spec.Completion.Timeout.Duration
	return d > 0 && task.Status.StartTime != nil && time.Since(task.Status.StartTime.Time) > d
}

func podExitMessage(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			return fmt.Sprintf("container %s exited %d", cs.Name, cs.State.Terminated.ExitCode)
		}
	}
	return "task Pod failed without a container exit code"
}

// isTerminalTaskPhase covers the phases that only wait for TTL. Failed is
// terminal too once settle() has stamped completionTime; the crash-interrupted
// retry case is filtered before this check in Reconcile.
func isTerminalTaskPhase(p kaalmv1beta1.AgentTaskPhase) bool {
	switch p {
	case kaalmv1beta1.TaskSucceeded, kaalmv1beta1.TaskFailed,
		kaalmv1beta1.TaskTimedOut, kaalmv1beta1.TaskTerminating:
		return true
	}
	return false
}

// reconcileDelete implements the task finalizer: gracefully terminate the Pod
// if one exists; everything else is owner-referenced and cascade-GCed.
func (r *AgentTaskReconciler) reconcileDelete(ctx context.Context, task *kaalmv1beta1.AgentTask) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(task, kaalmv1beta1.TaskFinalizer) {
		return ctrl.Result{}, nil
	}
	pod, err := r.ownedTaskPod(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod != nil {
		if pod.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	controllerutil.RemoveFinalizer(task, kaalmv1beta1.TaskFinalizer)
	return ctrl.Result{}, r.Update(ctx, task)
}

func (r *AgentTaskReconciler) setTaskPhase(task *kaalmv1beta1.AgentTask, phase kaalmv1beta1.AgentTaskPhase) {
	task.Status.Phase = phase
}

func (r *AgentTaskReconciler) setTaskReady(task *kaalmv1beta1.AgentTask, ok bool, reason, msg string) {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: kaalmv1beta1.ConditionReady, Status: status, Reason: reason, Message: msg,
	})
}

// SetupWithManager wires the reconciler, its owned children (including the
// completion mailbox and its RBAC pair), and the AgentClass map-func watch.
func (r *AgentTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		For(&kaalmv1beta1.AgentTask{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&cmapi.Certificate{}).
		Watches(&kaalmv1beta1.AgentClass{}, handler.EnqueueRequestsFromMapFunc(r.tasksForClass)).
		Complete(r)
}

func (r *AgentTaskReconciler) tasksForClass(ctx context.Context, obj client.Object) []reconcile.Request {
	var tasks kaalmv1beta1.AgentTaskList
	if err := r.List(ctx, &tasks, client.MatchingFields{IndexAgentClassRef: obj.GetName()}); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(tasks.Items))
	for _, t := range tasks.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: t.Namespace, Name: t.Name}})
	}
	return reqs
}
