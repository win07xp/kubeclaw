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
	"strings"
	"time"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

const (
	// certWaitRequeue is the backoff while waiting for cert-manager to issue
	// the per-Agent Certificate (first issuance typically takes seconds).
	certWaitRequeue = 5 * time.Second
	// crashLoopThreshold is the restart count at which a CrashLoopBackOff
	// container marks the Agent Failed.
	crashLoopThreshold = 5
)

// gateRequeue is the retry interval for Ready=False gates that depend on
// unwatched resources (imagePullSecrets and existingClaim PVCs): without it a
// Secret created after the gate fired would never be observed. A variable so
// tests can shorten it.
var gateRequeue = 30 * time.Second

// AgentReconciler owns the full child-resource tree for a persistent agent:
// Certificate, ServiceAccount, Service, PVC, NetworkPolicy, and the Pod, with
// Pod creation gated on certificate readiness. It drives the Pending ->
// Provisioning -> Running path of the Agent state machine plus Degraded,
// Failed, and Terminating. The Idle/Hibernation cycle, activity fan-out, and
// wake handling are gateway-coupled and land in a later phase. See
// docs/src/controller/reconcilers.md (AgentReconciler).
type AgentReconciler struct {
	client.Client
	Recorder record.EventRecorder
	// OperatorNamespace hosts the gateway and controller (kaalm-system).
	// Agents in this namespace are rejected to protect SAN integrity.
	OperatorNamespace string
	// MaxConcurrentReconciles is the number of reconciles that may run at
	// once; controller-runtime still serializes per object. 0 means one.
	MaxConcurrentReconciles int
	// Activity fetches per-namespace gateway activity for idle detection.
	// nil disables idle and hibernation transitions (no data, no evidence).
	Activity ActivityClient
	// Clock is injectable for tests; nil means time.Now.
	Clock func() time.Time
}

func (r *AgentReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=kaalm.io,resources=agents,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=kaalm.io,resources=agents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kaalm.io,resources=agents/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete

// Reconcile runs one pass of the Agent state machine.
func (r *AgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var agent kaalmv1beta1.Agent
	if err := r.Get(ctx, req.NamespacedName, &agent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !agent.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &agent)
	}

	if controllerutil.AddFinalizer(&agent, kaalmv1beta1.AgentFinalizer) {
		if err := r.Update(ctx, &agent); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	agent.Status.ObservedGeneration = agent.Generation
	if agent.Status.Phase == "" {
		r.setPhase(&agent, kaalmv1beta1.AgentPending)
	}

	// Step 9: wake-annotation handling, with phase-dependent removal so a
	// failed reconcile cannot silently drop the wake.
	if agent.Annotations[kaalmv1beta1.AnnotationWake] == kaalmv1beta1.AnnotationTrue {
		return r.handleWake(ctx, &agent)
	}

	// Step 1: the system namespace is forbidden (SAN-integrity guard).
	if agent.Namespace == r.OperatorNamespace {
		r.setReady(&agent, false, kaalmv1beta1.ReasonSystemNamespaceForbidden,
			fmt.Sprintf("Agents may not run in the operator namespace %q", r.OperatorNamespace))
		return ctrl.Result{}, r.Status().Update(ctx, &agent)
	}

	// Step 1 continued: resolve the AgentClass.
	var class kaalmv1beta1.AgentClass
	if err := r.Get(ctx, types.NamespacedName{Name: agent.Spec.AgentClassRef.Name}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			r.setReady(&agent, false, kaalmv1beta1.ReasonInvalidReference,
				fmt.Sprintf("AgentClass %q does not exist", agent.Spec.AgentClassRef.Name))
			return ctrl.Result{}, r.Status().Update(ctx, &agent)
		}
		return ctrl.Result{}, err
	}

	eff := deriveEffectiveSpec(&agent, &class)

	// Steps 2 and 5: the Degraded-triggering cross-checks (rules 2, 4, 5, 24,
	// 26, 29). All outstanding reasons are evaluated together so recovery can
	// be per-condition.
	if handled, res, err := r.reconcileDegraded(ctx, &agent, &class, eff); handled {
		return res, err
	}

	// Step 10 (S10): surface budget exhaustion as a Degraded condition without
	// a phase transition. Runs for every non-hard-degraded agent; the mutation
	// is persisted by whichever Status().Update the reconcile path below hits.
	r.reconcileBudgetCondition(ctx, &agent)

	// Hibernation phases: Hibernating drives the Pod down; Hibernated holds
	// with no Pod (PVC, Service, Certificate, SA, and NetworkPolicy persist).
	switch agent.Status.Phase {
	case kaalmv1beta1.AgentHibernating:
		return r.driveHibernating(ctx, &agent)
	case kaalmv1beta1.AgentHibernated:
		return ctrl.Result{}, r.Status().Update(ctx, &agent)
	}

	// Step 5: Ready=False gates that block Pod creation without degrading.
	gated, gateResult, err := r.readyGates(ctx, &agent, eff)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gated {
		if err := r.Status().Update(ctx, &agent); err != nil {
			return ctrl.Result{}, err
		}
		return gateResult, nil
	}

	// Step 4: ensure the Certificate and gate Pod creation on its readiness.
	certReady, err := r.ensureCertificate(ctx, &agent)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !certReady {
		if agent.Status.Phase == kaalmv1beta1.AgentPending {
			r.setPhase(&agent, kaalmv1beta1.AgentProvisioning)
		}
		r.setReady(&agent, false, "CertificateNotReady", "waiting for cert-manager to issue the agent certificate")
		if err := r.Status().Update(ctx, &agent); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: certWaitRequeue}, nil
	}

	// Step 6: converge the non-Pod children.
	if err := r.ensureChildren(ctx, &agent, &class, eff); err != nil {
		return ctrl.Result{}, err
	}

	// Steps 3, 6, 7: converge the Pod and derive the phase from it.
	if err := r.convergePod(ctx, &agent, eff); err != nil {
		return ctrl.Result{}, err
	}

	// Step 8: activity evaluation for Running and Idle agents drives the
	// idle and hibernation transitions.
	res := ctrl.Result{}
	if (agent.Status.Phase == kaalmv1beta1.AgentRunning || agent.Status.Phase == kaalmv1beta1.AgentIdle) &&
		eff.IdleTimeout > 0 && r.Activity != nil {
		res = r.evaluateActivity(ctx, &agent, eff)
	}

	if err := r.Status().Update(ctx, &agent); err != nil {
		return ctrl.Result{}, err
	}
	logger.V(1).Info("reconciled Agent", "phase", agent.Status.Phase)
	return res, nil
}

// handleWake implements the phase-dependent wake-annotation protocol: on a
// Hibernated agent the Resuming transition is committed BEFORE the annotation
// is removed, so an apiserver failure between the two leaves the wake intent
// observable; on any other phase the annotation is removed immediately, with
// a WakeIgnored warning except in Resuming (a benign idempotent re-attempt).
func (r *AgentReconciler) handleWake(ctx context.Context, agent *kaalmv1beta1.Agent) (ctrl.Result, error) {
	if agent.Status.Phase == kaalmv1beta1.AgentHibernated {
		r.setPhase(agent, kaalmv1beta1.AgentResuming)
		agent.Status.HibernatedAt = nil
		// The message that woke the agent is activity. The gateway records it
		// only once delivery succeeds, which is after the Pod is Ready, and
		// the controller's activity read is cached per namespace, so the
		// first Running pass would otherwise see only the pre-sleep record
		// and send the agent straight back through Idle to Hibernating.
		// evaluateActivity floors the gateway's record with this stamp.
		woke := metav1.NewTime(r.now())
		agent.Status.LastActivityTime = &woke
		r.setReady(agent, false, kaalmv1beta1.ReasonWoken, "wake requested; recreating the Pod")
		if err := r.Status().Update(ctx, agent); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Event(agent, corev1.EventTypeNormal, kaalmv1beta1.ReasonWoken, "waking from hibernation")
		wakesTotal.WithLabelValues(agent.Namespace, "activator").Inc()
		delete(agent.Annotations, kaalmv1beta1.AnnotationWake)
		if err := r.Update(ctx, agent); err != nil {
			// The next reconcile observes the annotation on a Resuming agent
			// and removes it silently.
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	phase := agent.Status.Phase
	delete(agent.Annotations, kaalmv1beta1.AnnotationWake)
	if err := r.Update(ctx, agent); err != nil {
		return ctrl.Result{}, err
	}
	if phase != kaalmv1beta1.AgentResuming {
		r.Recorder.Event(agent, corev1.EventTypeWarning, kaalmv1beta1.ReasonWakeIgnored,
			"wake annotation observed on non-Hibernated agent; ignored")
	}
	return ctrl.Result{Requeue: true}, nil
}

// driveHibernating deletes the Pod (gracefully) and settles Hibernated once
// it is gone. Everything else survives for wake-on-demand.
func (r *AgentReconciler) driveHibernating(ctx context.Context, agent *kaalmv1beta1.Agent) (ctrl.Result, error) {
	pod, err := r.ownedPod(ctx, agent)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod != nil {
		if pod.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, r.Status().Update(ctx, agent)
	}
	r.setPhase(agent, kaalmv1beta1.AgentHibernated)
	now := metav1.NewTime(r.now())
	agent.Status.HibernatedAt = &now
	agent.Status.PodName = ""
	r.setReady(agent, false, kaalmv1beta1.ReasonHibernated, "Pod deleted; PVC retained for wake")
	r.Recorder.Event(agent, corev1.EventTypeNormal, kaalmv1beta1.ReasonHibernated,
		"hibernated: Pod deleted, state retained")
	hibernationsTotal.WithLabelValues(agent.Namespace).Inc()
	return ctrl.Result{}, r.Status().Update(ctx, agent)
}

// evaluateActivity reads the gateway activity data and drives Running <->
// Idle and Idle -> Hibernating. Absence of data is not evidence of
// inactivity: unreachable gateways preserve the phase, and silence counts
// only once a replica has been up for idleTimeout.
func (r *AgentReconciler) evaluateActivity(
	ctx context.Context, agent *kaalmv1beta1.Agent, eff effectiveAgentSpec,
) ctrl.Result {
	now := r.now()
	reachable, total, err := r.Activity.NamespaceActivity(ctx, agent.Namespace)
	if err != nil || total == 0 || len(reachable) == 0 {
		apimeta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type: kaalmv1beta1.ConditionGatewayReachable, Status: metav1.ConditionFalse,
			Reason: "GatewayUnavailable", Message: "no gateway activity data; idle transitions deferred",
		})
		return ctrl.Result{RequeueAfter: 30 * time.Second}
	}
	apimeta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type: kaalmv1beta1.ConditionGatewayReachable, Status: metav1.ConditionTrue,
		Reason: "GatewayReady", Message: "gateway activity data available",
	})

	last := mergedActivity(reachable, agent.Name, eff.ActivitySource)
	synthetic := false
	if last == nil {
		// No recorded activity: genuine silence only if some replica has
		// been watching for at least idleTimeout; otherwise a restart wiped
		// the store and the data is unknown.
		qualified := false
		for _, replica := range reachable {
			if now.Sub(replica.StartedAt) >= eff.IdleTimeout {
				qualified = true
				break
			}
		}
		if !qualified || agent.Status.PhaseTransitionTime == nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}
		}
		// Fall back to PhaseTransitionTime as the silence marker, but remember
		// it is synthetic: it advances on every setPhase, so it must not be
		// read as fresh activity below (that would oscillate Idle<->Running).
		t := agent.Status.PhaseTransitionTime.Time
		last = &t
		synthetic = true
	}

	switch agent.Status.Phase {
	case kaalmv1beta1.AgentRunning:
		// A wake stamps LastActivityTime (handleWake). Floor the gateway's
		// record with it, so a woken agent gets a full idleTimeout while the
		// gateway still reports only the activity that preceded its sleep.
		marker := *last
		if agent.Status.LastActivityTime != nil && agent.Status.LastActivityTime.After(marker) {
			marker = agent.Status.LastActivityTime.Time
		}
		if now.Sub(marker) > eff.IdleTimeout {
			r.setPhase(agent, kaalmv1beta1.AgentIdle)
			lt := metav1.NewTime(marker)
			agent.Status.LastActivityTime = &lt
			r.Recorder.Event(agent, corev1.EventTypeNormal, kaalmv1beta1.ReasonPhaseChanged,
				fmt.Sprintf("idle: no activity for %s", eff.IdleTimeout))
			// Re-enter promptly: an already-elapsed hibernationDelay should
			// carry straight on to Hibernating on the next pass.
			return ctrl.Result{Requeue: true}
		}
	case kaalmv1beta1.AgentIdle:
		// Only real recorded activity returns an Idle agent to Running. The
		// synthetic fallback marker advances on every transition, so treating
		// it as fresh activity would flip Idle->Running->Idle forever and never
		// let a trafficless agent hibernate.
		//
		// Compare at second resolution: the stored timestamp lost its
		// nanoseconds in the apiserver round-trip (metav1.Time marshals to
		// RFC3339 seconds), and a raw comparison would misread the same
		// instant as fresh activity on every pass.
		if !synthetic && agent.Status.LastActivityTime != nil &&
			last.Truncate(time.Second).After(agent.Status.LastActivityTime.Time) {
			r.setPhase(agent, kaalmv1beta1.AgentRunning)
			lt := metav1.NewTime(*last)
			agent.Status.LastActivityTime = &lt
			return ctrl.Result{Requeue: true}
		}
		// Measure the hibernation window from a stable silence marker. The
		// synthetic path uses LastActivityTime (stamped once at the Idle
		// transition), not the advancing PhaseTransitionTime, so the delay is
		// counted from when silence began rather than reset on every pass.
		// The real path floors the gateway's record with LastActivityTime for
		// the same reason as the Running case: after a wake the record still
		// predates the sleep, and the delay must count from the wake.
		silenceStart := last
		if agent.Status.LastActivityTime != nil &&
			(synthetic || agent.Status.LastActivityTime.After(*silenceStart)) {
			silenceStart = &agent.Status.LastActivityTime.Time
		}
		if eff.HibernationEnabled && now.Sub(*silenceStart) > eff.IdleTimeout+eff.HibernationDelay {
			r.setPhase(agent, kaalmv1beta1.AgentHibernating)
			r.Recorder.Event(agent, corev1.EventTypeNormal, kaalmv1beta1.ReasonPhaseChanged,
				fmt.Sprintf("hibernating: idle for %s past the idle timeout", eff.HibernationDelay))
			return ctrl.Result{Requeue: true}
		}
	}
	return ctrl.Result{RequeueAfter: activityCacheWindow}
}

// readyGates evaluates the Ready=False conditions that block Pod creation
// without degrading: a missing image, a missing existingClaim, missing
// imagePullSecrets, and a missing handler ConfigMap (rule 31). It sets the
// condition on the Agent and reports whether the pass is gated; Secrets, PVCs,
// and handler ConfigMaps are unwatched, so gated results carry a requeue
// interval.
func (r *AgentReconciler) readyGates(
	ctx context.Context, agent *kaalmv1beta1.Agent, eff effectiveAgentSpec,
) (bool, ctrl.Result, error) {
	if eff.Image == "" {
		r.setReady(agent, false, kaalmv1beta1.ReasonInvalidReference,
			"no image: Agent.spec.image is empty and the AgentClass sets no defaultImage")
		return true, ctrl.Result{}, nil
	}
	if eff.PersistenceOn && eff.ExistingClaim != "" {
		var pvc corev1.PersistentVolumeClaim
		err := r.Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: eff.ExistingClaim}, &pvc)
		if apierrors.IsNotFound(err) {
			r.setReady(agent, false, kaalmv1beta1.ReasonExistingClaimNotFound,
				fmt.Sprintf("existingClaim %q not found in namespace %q", eff.ExistingClaim, agent.Namespace))
			return true, ctrl.Result{RequeueAfter: gateRequeue}, nil
		} else if err != nil {
			return false, ctrl.Result{}, err
		}
	}
	for _, ref := range eff.ImagePullSecrets {
		var sec corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: ref.Name}, &sec)
		if apierrors.IsNotFound(err) {
			r.setReady(agent, false, kaalmv1beta1.ReasonImagePullSecretMissing,
				fmt.Sprintf("imagePullSecret %q missing in namespace %q", ref.Name, agent.Namespace))
			return true, ctrl.Result{RequeueAfter: gateRequeue}, nil
		} else if err != nil {
			return false, ctrl.Result{}, err
		}
	}
	// Rule 31: the handler ConfigMap must exist where the Agent runs. Checked
	// pre-Pod so a bad reference surfaces as a condition, not a Pod wedged in
	// ContainerCreating on a missing volume source. The ConfigMap is
	// developer-owned: no ownerRef is added and content is never tracked.
	if eff.HandlerConfigMap != "" {
		var cm corev1.ConfigMap
		err := r.Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: eff.HandlerConfigMap}, &cm)
		if apierrors.IsNotFound(err) {
			r.setReady(agent, false, kaalmv1beta1.ReasonHandlerConfigMapNotFound,
				fmt.Sprintf("handler ConfigMap %q not found in namespace %q", eff.HandlerConfigMap, agent.Namespace))
			return true, ctrl.Result{RequeueAfter: gateRequeue}, nil
		} else if err != nil {
			return false, ctrl.Result{}, err
		}
	}
	return false, ctrl.Result{}, nil
}

// reconcileDegraded runs the cross-checks and drives the Degraded enter/leave
// protocol. handled=true means the pass ends here (either the agent is
// Degraded, or it just recovered and requeues).
func (r *AgentReconciler) reconcileDegraded(
	ctx context.Context, agent *kaalmv1beta1.Agent, class *kaalmv1beta1.AgentClass, eff effectiveAgentSpec,
) (bool, ctrl.Result, error) {
	reasons := r.degradedReasons(ctx, agent, class, eff)
	if len(reasons) > 0 {
		return true, ctrl.Result{}, r.enterOrStayDegraded(ctx, agent, reasons)
	}
	if agent.Status.Phase != kaalmv1beta1.AgentDegraded {
		return false, ctrl.Result{}, nil
	}
	// Every Degraded-triggering condition has cleared: restore the prior
	// phase and null preDegradedPhase atomically in the same write.
	restored := agent.Status.PreDegradedPhase
	if restored == "" {
		restored = kaalmv1beta1.AgentPending
	}
	r.setPhase(agent, restored)
	agent.Status.PreDegradedPhase = ""
	r.Recorder.Event(agent, corev1.EventTypeNormal, kaalmv1beta1.ReasonPhaseChanged,
		fmt.Sprintf("recovered from Degraded to %s", restored))
	return true, ctrl.Result{Requeue: true}, r.Status().Update(ctx, agent)
}

// degradedReasons evaluates every Degraded-triggering cross-check and returns
// the outstanding reasons in a stable order (first entry becomes the reported
// reason).
func (r *AgentReconciler) degradedReasons(
	ctx context.Context, agent *kaalmv1beta1.Agent, class *kaalmv1beta1.AgentClass, eff effectiveAgentSpec,
) []metav1.Condition {
	var out []metav1.Condition
	add := func(reason, msg string) {
		out = append(out, metav1.Condition{Reason: reason, Message: msg})
	}

	// Rule 2: image allowlist.
	if eff.Image != "" && !imageAllowed(eff.Image, class.Spec.Image.AllowedImages) {
		add(kaalmv1beta1.ReasonClassConstraintViolation,
			fmt.Sprintf("image %q does not match AgentClass %q allowedImages", eff.Image, class.Name))
	}
	// Rules 4, 5: provider resolution, allowlist, and namespace admission.
	for _, p := range agent.Spec.Providers {
		name := p.ProviderRef.Name
		// An empty allowedProviders list allows none (docs/src/resources/agentclass.md).
		allowed := false
		for _, ap := range class.Spec.AllowedProviders {
			if ap.Name == name {
				allowed = true
				break
			}
		}
		if !allowed {
			add(kaalmv1beta1.ReasonClassConstraintViolation,
				fmt.Sprintf("provider %q is not in AgentClass %q allowedProviders", name, class.Name))
			continue
		}
		var mp kaalmv1beta1.ModelProvider
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &mp); err != nil {
			if apierrors.IsNotFound(err) {
				add(kaalmv1beta1.ReasonClassConstraintViolation,
					fmt.Sprintf("provider %q does not exist", name))
			}
			continue
		}
		if !namespaceAllowed(agent.Namespace, mp.Spec.AllowedNamespaces) {
			add(kaalmv1beta1.ReasonClassConstraintViolation,
				fmt.Sprintf("provider %q does not allow namespace %q", name, agent.Namespace))
		}
	}
	// Rules 35 to 38: tool grant resolution, class allowlist, namespace
	// admission, and catalog membership.
	out = append(out, toolGrantViolations(ctx, r.Client, agent.Namespace, agent.Spec.Tools, class)...)
	// Rule 24: persistence must be class-permitted.
	if agent.Spec.Persistence.Enabled && !class.Spec.Persistence.Enabled {
		add(kaalmv1beta1.ReasonPersistenceNotAllowed,
			fmt.Sprintf("persistence requested but AgentClass %q has persistence.enabled=false", class.Name))
	}
	// Rule 26: hibernation must be class-permitted.
	if agent.Spec.Lifecycle.HibernationEnabled && !class.Spec.Lifecycle.HibernationAllowed {
		add(kaalmv1beta1.ReasonHibernationNotAllowed,
			fmt.Sprintf("hibernation requested but AgentClass %q has lifecycle.hibernationAllowed=false", class.Name))
	}
	// Rule 29: hibernation requires persistence (spec-internal).
	if agent.Spec.Lifecycle.HibernationEnabled && !agent.Spec.Persistence.Enabled {
		add(kaalmv1beta1.ReasonHibernationRequiresPersist,
			"lifecycle.hibernationEnabled=true requires spec.persistence.enabled=true")
	}
	// Rule 30: a handler mount must be class-permitted. Drift-sensitive like
	// rules 24 and 26: flipping allowHandlerMounts off on a live class
	// degrades existing handler-mounting Agents.
	if agent.Spec.Handler != nil && !class.Spec.Image.AllowHandlerMounts {
		add(kaalmv1beta1.ReasonHandlerMountNotAllowed,
			fmt.Sprintf("spec.handler set but AgentClass %q has image.allowHandlerMounts=false", class.Name))
	}
	return out
}

// enterOrStayDegraded transitions into Degraded (recording preDegradedPhase on
// first entry only) or refreshes reason/message while already Degraded.
func (r *AgentReconciler) enterOrStayDegraded(
	ctx context.Context, agent *kaalmv1beta1.Agent, reasons []metav1.Condition,
) error {
	first := reasons[0]
	if agent.Status.Phase != kaalmv1beta1.AgentDegraded {
		agent.Status.PreDegradedPhase = agent.Status.Phase
		r.setPhase(agent, kaalmv1beta1.AgentDegraded)
		r.Recorder.Event(agent, corev1.EventTypeWarning, first.Reason, first.Message)
	}
	r.setReady(agent, false, first.Reason, first.Message)
	return r.Status().Update(ctx, agent)
}

// reconcileBudgetCondition mirrors provider budget state onto the Agent (S10).
// If any referenced ModelProvider reports the Agent's namespace as budget
// Blocked in status.budgetUsage, the Degraded condition is set (reason
// BudgetExhausted); otherwise it is removed. This never touches status.phase:
// budget exhaustion is a recoverable runtime state, not a lifecycle transition,
// so the agent keeps running and the signal clears on its own when the provider
// reports the namespace unblocked (period reset, budget increase, or spend
// drop), driven by the ModelProvider watch. Provider Get errors are tolerated
// (a missing or unreadable provider is the degrade path's concern, not this
// one): the condition reflects what could be read.
func (r *AgentReconciler) reconcileBudgetCondition(ctx context.Context, agent *kaalmv1beta1.Agent) {
	var blocking []string
	for _, p := range agent.Spec.Providers {
		var mp kaalmv1beta1.ModelProvider
		if err := r.Get(ctx, types.NamespacedName{Name: p.ProviderRef.Name}, &mp); err != nil {
			continue
		}
		for _, u := range mp.Status.BudgetUsage {
			if u.Namespace == agent.Namespace && u.State == kaalmv1beta1.BudgetStateBlocked {
				blocking = append(blocking, mp.Name)
				break
			}
		}
	}
	if len(blocking) == 0 {
		apimeta.RemoveStatusCondition(&agent.Status.Conditions, kaalmv1beta1.ConditionDegraded)
		return
	}
	apimeta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               kaalmv1beta1.ConditionDegraded,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: agent.Generation,
		Reason:             kaalmv1beta1.ReasonBudgetExhausted,
		Message: fmt.Sprintf("namespace %q budget exhausted on provider %s",
			agent.Namespace, strings.Join(blocking, ", ")),
	})
}

func (r *AgentReconciler) ensureCertificate(ctx context.Context, agent *kaalmv1beta1.Agent) (bool, error) {
	var cert cmapi.Certificate
	key := types.NamespacedName{Namespace: agent.Namespace, Name: agentCertificateName(agent.Name)}
	if err := r.Get(ctx, key, &cert); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		desired := desiredCertificate(agent)
		if err := controllerutil.SetControllerReference(agent, desired, r.Scheme()); err != nil {
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

// ensureChildren converges the ServiceAccount, Service, PVC, and
// NetworkPolicy (everything except the Certificate and the Pod).
func (r *AgentReconciler) ensureChildren(
	ctx context.Context, agent *kaalmv1beta1.Agent, class *kaalmv1beta1.AgentClass, eff effectiveAgentSpec,
) error {
	if err := r.ensureServiceAccount(ctx, agent); err != nil {
		return err
	}
	if eff.ServiceEnabled {
		if err := r.ensureService(ctx, agent, eff); err != nil {
			return err
		}
	}
	if eff.PersistenceOn && eff.ExistingClaim == "" {
		if err := r.ensurePVC(ctx, agent, class, eff); err != nil {
			return err
		}
	}
	return r.ensureNetworkPolicy(ctx, agent, class, eff)
}

func (r *AgentReconciler) ensureServiceAccount(ctx context.Context, agent *kaalmv1beta1.Agent) error {
	desired := desiredServiceAccount(agent)
	if err := controllerutil.SetControllerReference(agent, desired, r.Scheme()); err != nil {
		return err
	}
	err := r.Create(ctx, desired)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (r *AgentReconciler) ensureService(ctx context.Context, agent *kaalmv1beta1.Agent, eff effectiveAgentSpec) error {
	desired := desiredService(agent, eff)
	if err := controllerutil.SetControllerReference(agent, desired, r.Scheme()); err != nil {
		return err
	}
	var current corev1.Service
	key := types.NamespacedName{Namespace: desired.Namespace, Name: desired.Name}
	if err := r.Get(ctx, key, &current); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
	} else if len(current.Spec.Ports) != 1 ||
		current.Spec.Ports[0].Port != desired.Spec.Ports[0].Port ||
		current.Spec.Ports[0].TargetPort != desired.Spec.Ports[0].TargetPort {
		current.Spec.Ports = desired.Spec.Ports
		if err := r.Update(ctx, &current); err != nil {
			return err
		}
	}
	agent.Status.Endpoint = fmt.Sprintf("https://%s.%s.svc.cluster.local:%d", agent.Name, agent.Namespace, eff.ServicePort)
	return nil
}

func (r *AgentReconciler) ensurePVC(
	ctx context.Context, agent *kaalmv1beta1.Agent, class *kaalmv1beta1.AgentClass, eff effectiveAgentSpec,
) error {
	desired := desiredPVC(agent, class, eff)
	if err := controllerutil.SetControllerReference(agent, desired, r.Scheme()); err != nil {
		return err
	}
	err := r.Create(ctx, desired)
	if apierrors.IsAlreadyExists(err) {
		err = nil
	}
	if err == nil {
		agent.Status.PVCName = desired.Name
	}
	return err
}

func (r *AgentReconciler) ensureNetworkPolicy(
	ctx context.Context, agent *kaalmv1beta1.Agent, class *kaalmv1beta1.AgentClass, eff effectiveAgentSpec,
) error {
	desired := desiredNetworkPolicy(agent, class, eff, r.OperatorNamespace)
	if err := controllerutil.SetControllerReference(agent, desired, r.Scheme()); err != nil {
		return err
	}
	var current networkingv1.NetworkPolicy
	key := types.NamespacedName{Namespace: desired.Namespace, Name: desired.Name}
	if err := r.Get(ctx, key, &current); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return r.Create(ctx, desired)
	}
	current.Spec = desired.Spec
	return r.Update(ctx, &current)
}

// convergePod implements Pod convergence: create when missing, replace when
// terminal (involuntary disruption) or when the spec hash drifts, mark the
// Agent Failed on a persistent crash loop, and derive Running from readiness.
func (r *AgentReconciler) convergePod(
	ctx context.Context, agent *kaalmv1beta1.Agent, eff effectiveAgentSpec,
) error {
	pod, err := r.ownedPod(ctx, agent)
	if err != nil {
		return err
	}

	if pod == nil {
		desired := desiredPod(agent, eff, r.OperatorNamespace)
		if err := controllerutil.SetControllerReference(agent, desired, r.Scheme()); err != nil {
			return err
		}
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
		r.setPhase(agent, kaalmv1beta1.AgentProvisioning)
		r.setReady(agent, false, "PodProvisioning", "agent Pod created, waiting for readiness")
		agent.Status.PodName = desired.Name
		return nil
	}

	// A Pod already being deleted is a replacement in progress: wait for the
	// owned-Pod watch to fire when it is gone.
	if !pod.DeletionTimestamp.IsZero() {
		r.setPhase(agent, kaalmv1beta1.AgentProvisioning)
		r.setReady(agent, false, "PodProvisioning", "previous Pod terminating")
		return nil
	}

	// Involuntary disruption: a terminal Pod is never resurrected by the
	// kubelet under restartPolicy Always, so delete it and re-provision.
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		r.Recorder.Event(agent, corev1.EventTypeWarning, "PodDisrupted",
			fmt.Sprintf("Pod %s is terminal (%s); re-provisioning", pod.Name, pod.Status.Phase))
		r.setPhase(agent, kaalmv1beta1.AgentProvisioning)
		r.setReady(agent, false, "PodDisrupted", "replacing a terminal Pod")
		return r.Delete(ctx, pod)
	}

	// Persistent crash loop marks the Agent Failed (any -> Failed).
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil &&
			(cs.State.Waiting.Reason == "CrashLoopBackOff" && cs.RestartCount >= crashLoopThreshold ||
				cs.State.Waiting.Reason == "ImagePullBackOff") {
			r.setPhase(agent, kaalmv1beta1.AgentFailed)
			r.setReady(agent, false, cs.State.Waiting.Reason,
				fmt.Sprintf("container %s: %s", cs.Name, cs.State.Waiting.Message))
			return nil
		}
	}

	// Spec drift: compare the stamped hash against the re-derived one, never
	// the live Pod object.
	if pod.Annotations[annotationPodSpecHash] != podSpecHash(eff) {
		r.Recorder.Event(agent, corev1.EventTypeNormal, "SpecDrift",
			"derived Pod spec changed; replacing the Pod")
		r.setPhase(agent, kaalmv1beta1.AgentProvisioning)
		r.setReady(agent, false, "SpecDrift", "replacing Pod for updated spec")
		return r.Delete(ctx, pod)
	}

	agent.Status.PodName = pod.Name
	if podReady(pod) {
		// Idle is a pod-bearing phase: a ready Pod does not promote an Idle
		// agent back to Running; only fresh activity does.
		if agent.Status.Phase != kaalmv1beta1.AgentIdle {
			r.setPhase(agent, kaalmv1beta1.AgentRunning)
		}
		r.setReady(agent, true, kaalmv1beta1.ReasonPodRunning, "agent Pod is ready")
	} else {
		r.setPhase(agent, kaalmv1beta1.AgentProvisioning)
		r.setReady(agent, false, "PodNotReady", "agent Pod is not ready")
	}
	return nil
}

// ownedPod returns the Agent's live Pod, preferring a non-terminating one when
// a replacement overlaps a termination.
func (r *AgentReconciler) ownedPod(ctx context.Context, agent *kaalmv1beta1.Agent) (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(agent.Namespace),
		client.MatchingLabels(agentPodLabels(agent))); err != nil {
		return nil, err
	}
	var candidate *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if !metav1.IsControlledBy(p, agent) {
			continue
		}
		if p.DeletionTimestamp.IsZero() {
			return p, nil
		}
		candidate = p
	}
	return candidate, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// reconcileDelete implements the agent finalizer: gracefully terminate the Pod
// if one exists, apply pvcRetention by rewriting the PVC's ownerRef, then
// release the finalizer. See docs/src/controller/finalizers.md (Agent).
func (r *AgentReconciler) reconcileDelete(ctx context.Context, agent *kaalmv1beta1.Agent) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(agent, kaalmv1beta1.AgentFinalizer) {
		return ctrl.Result{}, nil
	}

	if agent.Status.Phase != kaalmv1beta1.AgentTerminating {
		agent.Status.PreDegradedPhase = ""
		r.setPhase(agent, kaalmv1beta1.AgentTerminating)
		if err := r.Status().Update(ctx, agent); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Terminate the Pod gracefully and wait for it to go away; the owned-Pod
	// watch re-enqueues us when it does.
	pod, err := r.ownedPod(ctx, agent)
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

	// pvcRetention: Retain strips the PVC's ownerRef before the finalizer is
	// removed, so cascade GC finds no owner and leaves the PVC in place. The
	// ownerRef edit must land before the finalizer removal: once the finalizer
	// entry is gone the Agent can vanish at any moment. existingClaim PVCs
	// never carried an ownerRef and are untouched.
	var class kaalmv1beta1.AgentClass
	retention := "Delete"
	if err := r.Get(ctx, types.NamespacedName{Name: agent.Spec.AgentClassRef.Name}, &class); err == nil {
		if class.Spec.Persistence.PVCRetention != "" {
			retention = class.Spec.Persistence.PVCRetention
		}
	}
	if retention == "Retain" {
		var pvc corev1.PersistentVolumeClaim
		key := types.NamespacedName{Namespace: agent.Namespace, Name: agentPVCName(agent.Name)}
		if err := r.Get(ctx, key, &pvc); err == nil {
			var kept []metav1.OwnerReference
			for _, ref := range pvc.OwnerReferences {
				if ref.UID != agent.UID {
					kept = append(kept, ref)
				}
			}
			if len(kept) != len(pvc.OwnerReferences) {
				pvc.OwnerReferences = kept
				if err := r.Update(ctx, &pvc); err != nil {
					return ctrl.Result{}, err
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	controllerutil.RemoveFinalizer(agent, kaalmv1beta1.AgentFinalizer)
	return ctrl.Result{}, r.Update(ctx, agent)
}

func (r *AgentReconciler) setPhase(agent *kaalmv1beta1.Agent, phase kaalmv1beta1.AgentPhase) {
	if agent.Status.Phase == phase {
		return
	}
	agent.Status.Phase = phase
	now := metav1.Now()
	agent.Status.PhaseTransitionTime = &now
}

func (r *AgentReconciler) setReady(agent *kaalmv1beta1.Agent, ok bool, reason, msg string) {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type: kaalmv1beta1.ConditionReady, Status: status, Reason: reason, Message: msg,
	})
}

// SetupWithManager wires the reconciler, its owned children, and the
// platform-level map-func watches.
func (r *AgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		For(&kaalmv1beta1.Agent{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&cmapi.Certificate{}).
		Watches(&kaalmv1beta1.AgentClass{}, handler.EnqueueRequestsFromMapFunc(r.agentsForClass)).
		Watches(&kaalmv1beta1.ModelProvider{}, handler.EnqueueRequestsFromMapFunc(r.agentsForProvider)).
		Watches(&kaalmv1beta1.ToolProvider{}, handler.EnqueueRequestsFromMapFunc(r.agentsForToolProvider)).
		Complete(r)
}

func (r *AgentReconciler) agentsForClass(ctx context.Context, obj client.Object) []reconcile.Request {
	var agents kaalmv1beta1.AgentList
	if err := r.List(ctx, &agents, client.MatchingFields{IndexAgentClassRef: obj.GetName()}); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(agents.Items))
	for _, a := range agents.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}})
	}
	return reqs
}

func (r *AgentReconciler) agentsForProvider(ctx context.Context, obj client.Object) []reconcile.Request {
	var agents kaalmv1beta1.AgentList
	if err := r.List(ctx, &agents, client.MatchingFields{IndexProviderRef: obj.GetName()}); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(agents.Items))
	for _, a := range agents.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}})
	}
	return reqs
}

func (r *AgentReconciler) agentsForToolProvider(ctx context.Context, obj client.Object) []reconcile.Request {
	var agents kaalmv1beta1.AgentList
	if err := r.List(ctx, &agents, client.MatchingFields{IndexToolProviderRef: obj.GetName()}); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(agents.Items))
	for _, a := range agents.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}})
	}
	return reqs
}
