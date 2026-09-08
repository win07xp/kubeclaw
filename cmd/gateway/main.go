// Command gateway is the Kaalm Gateway: the cluster listener on :8443 (the
// LLM proxy, the MCP tool broker, and the internal endpoints, with per-path
// client authentication), the Ingress-fronted user listener on :8080, and a
// dedicated health port. See docs/src/gateways/.
package main

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
	"github.com/win07xp/kaalm/internal/callbackpolicy"
	"github.com/win07xp/kaalm/internal/gateway"
	"github.com/win07xp/kaalm/internal/tlsutil"
)

func main() {
	var (
		listenAddr           string
		healthAddr           string
		certFile, keyFile    string
		caFile               string
		upstreamCAFile       string
		callbackCAFile       string
		callbackAllowlist    string
		maxBodyBytes         int64
		upstreamTimeout      time.Duration
		disableSourceIPCheck bool
		userAddr             string
		agentHostOverride    string
		agentPortOverride    int
		metricsAddr          string
		otlpEndpoint         string
		otlpSampleRatio      float64
		maxFallbackDepth     int
		maxMessageBodyBytes  int64
		maxResponseBodyBytes int64
		syncDeliveryDeadline time.Duration
		agentReadTimeout     time.Duration
		agentConnectTimeout  time.Duration
		channelHealthWindow  time.Duration
		deliveryBackoff      string
		callbackBackoff      string
		discordAPIBaseURL    string
		whatsAppAPIBaseURL   string
	)
	flag.StringVar(&listenAddr, "listen-addr", ":8443", "cluster listener (:8443) address")
	flag.StringVar(&healthAddr, "health-addr", ":8081", "health listener address")
	flag.StringVar(&certFile, "tls-cert", "/var/run/kaalm/tls.crt", "serving certificate file")
	flag.StringVar(&keyFile, "tls-key", "/var/run/kaalm/tls.key", "serving key file")
	flag.StringVar(&caFile, "tls-ca", "/var/run/kaalm/ca.crt", "Kaalm CA bundle for client verification")
	flag.StringVar(&upstreamCAFile, "upstream-ca", "",
		"comma-separated CA bundle files to trust for upstream provider TLS, merged and added to the system roots")
	flag.StringVar(&callbackCAFile, "callback-ca", "",
		"comma-separated CA bundle files to trust for channel callbackUrl TLS, merged and added to the system roots")
	flag.StringVar(&callbackAllowlist, "callback-url-allowlist", "",
		"comma-separated DNS-name suffixes and CIDR blocks whose callbackUrl targets are permitted despite the "+
			"deny-internal default; loopback and cloud metadata stay blocked regardless")
	flag.Int64Var(&maxBodyBytes, "max-llm-body-bytes", 4<<20, "inbound LLM request body cap")
	flag.DurationVar(&upstreamTimeout, "upstream-timeout", 120*time.Second, "upstream provider call timeout")
	flag.BoolVar(&disableSourceIPCheck, "disable-source-ip-check", false,
		"skip the source-IP-to-Pod cross-check (dev only; the check is defense in depth and must stay on in-cluster)")
	flag.StringVar(&userAddr, "user-addr", ":8080", "User Gateway listener address")
	flag.StringVar(&agentHostOverride, "agent-host-override", "", "redirect agent delivery dials to this host (dev only)")
	flag.IntVar(&agentPortOverride, "agent-port-override", 0, "redirect agent delivery dials to this port (dev only)")
	flag.StringVar(&metricsAddr, "metrics-addr", ":9090", "Prometheus metrics listener address")
	flag.StringVar(&otlpEndpoint, "otlp-endpoint", "",
		"OTLP/HTTP trace exporter base URL (for example http://collector:4318); empty disables tracing entirely")
	flag.Float64Var(&otlpSampleRatio, "otlp-sample-ratio", 1.0,
		"parent-based head sampling ratio for traces this gateway starts")
	flag.IntVar(&maxFallbackDepth, "max-fallback-depth", 3, "total providers attempted per request, including the primary")
	flag.Int64Var(&maxMessageBodyBytes, "max-message-body-bytes", 1<<20, "inbound webhook body cap")
	flag.Int64Var(&maxResponseBodyBytes, "max-response-body-bytes", 900<<10, "agent reply body cap")
	flag.DurationVar(&syncDeliveryDeadline, "sync-delivery-deadline", 30*time.Second, "sync-mode wall-clock budget")
	flag.DurationVar(&agentReadTimeout, "agent-read-timeout", 10*time.Second, "per-attempt agent/callback read timeout")
	flag.DurationVar(&agentConnectTimeout, "agent-connect-timeout", time.Second, "agent-delivery connect timeout")
	flag.DurationVar(&channelHealthWindow, "channel-health-window", 5*time.Minute, "rolling window for PlatformConnected")
	flag.StringVar(&deliveryBackoff, "delivery-backoff", "1s,5s,25s", "agent-delivery retry backoff (comma-separated)")
	flag.StringVar(&callbackBackoff, "callback-backoff", "1s,5s,25s", "callback retry backoff schedule (comma-separated)")
	flag.StringVar(&discordAPIBaseURL, "platform-discord-api-base-url", gateway.DefaultDiscordAPIBaseURL,
		"base URL the Discord adapter replies through (gateway.platforms.discord.apiBaseUrl)")
	flag.StringVar(&whatsAppAPIBaseURL, "platform-whatsapp-api-base-url", gateway.DefaultWhatsAppAPIBaseURL,
		"base URL the WhatsApp adapter replies through, including the Graph API version "+
			"(gateway.platforms.whatsapp.apiBaseUrl)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	// Install it as the process default: the gateway packages log through
	// package-level slog (including the broker's per-call audit record), and
	// without this they would fall back to Go's plain text logger, breaking
	// the JSON log convention (docs/src/operations/observability.md).
	slog.SetDefault(logger)

	operatorNamespace := os.Getenv("POD_NAMESPACE")
	if operatorNamespace == "" {
		operatorNamespace = "kaalm-system"
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kaalmv1beta1.AddToScheme(scheme))
	utilruntime.Must(cmapi.AddToScheme(scheme))

	restCfg := ctrl.GetConfigOrDie()
	// Secrets are read uncached (direct GET), never through an informer. The
	// gateway holds only get/watch on Secrets in kaalm-system plus dynamic
	// resourceNames-scoped grants on individual channel Secrets (no cluster-wide
	// list), so a cached Secret informer would issue a forbidden cluster-scoped
	// LIST and the read would hang waiting for a sync that never lands. See
	// docs/src/security/rbac.md (gateway Secret access).
	cl, err := cluster.New(restCfg, func(o *cluster.Options) {
		o.Scheme = scheme
		o.Client.Cache = &client.CacheOptions{
			DisableFor: []client.Object{&corev1.Secret{}},
		}
		// The ConfigMap informer (budget watch-fold) must be scoped to the
		// operator namespace: the gateway's Role grants list/watch there
		// only, and an unscoped informer would issue a forbidden
		// cluster-wide LIST and hang, the same failure mode the Secret
		// comment above describes.
		o.Cache.ByObject = map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {Namespaces: map[string]cache.Config{
				operatorNamespace: {},
			}},
		}
	})
	if err != nil {
		logger.Error("building cluster cache", "error", err)
		os.Exit(1)
	}
	if err := cl.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, gateway.PodIPIndex,
		func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Status.PodIP == "" {
				return nil
			}
			return []string{pod.Status.PodIP}
		}); err != nil {
		logger.Error("registering pod IP index", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		logger.Error("building clientset", "error", err)
		os.Exit(1)
	}

	// The upstream and callback CA bundles are additive to the system roots:
	// they let the gateway reach in-cluster or self-hosted providers/receivers
	// signed by a private CA (e.g. kaalm-ca) without losing the ability to
	// verify public endpoints. The server re-reads them when their mtime
	// changes, so a rotated bundle needs no restart; validate once here so a
	// misconfigured path fails fast at startup rather than at first dial.
	for _, file := range splitPaths(upstreamCAFile) {
		validateCABundle(file, "upstream", logger)
	}
	for _, file := range splitPaths(callbackCAFile) {
		validateCABundle(file, "callback", logger)
	}

	// The MCP session key binds session ids to caller identities across
	// replicas; the chart provides it. A missing key gets a per-process
	// random fallback: sessions then survive only on this replica, which is
	// fine for local runs and loudly wrong for real ones.
	sessionKey := []byte(os.Getenv("KAALM_MCP_SESSION_KEY"))
	if len(sessionKey) == 0 {
		sessionKey = make([]byte, 32)
		if _, err := rand.Read(sessionKey); err != nil {
			logger.Error("generating fallback MCP session key", "error", err)
			os.Exit(1)
		}
		logger.Warn("KAALM_MCP_SESSION_KEY is not set; using a per-process key, " +
			"MCP sessions will not survive replica hops")
	}

	store := &gateway.KubeStore{
		Reader: cl.GetClient(), APIReader: cl.GetAPIReader(), OperatorNamespace: operatorNamespace,
	}
	tokens := gateway.NewTokenAuthenticator(&gateway.KubeTokenReviewer{Client: clientset})
	async := &gateway.KubeAsyncRecords{Client: clientset, OperatorNamespace: operatorNamespace}
	server := gateway.NewServer(gateway.Config{
		OperatorNamespace:        operatorNamespace,
		ListenAddr:               listenAddr,
		HealthAddr:               healthAddr,
		CertFile:                 certFile,
		KeyFile:                  keyFile,
		CAFile:                   caFile,
		MaxBodyBytes:             maxBodyBytes,
		UpstreamTimeout:          upstreamTimeout,
		SessionKey:               sessionKey,
		UpstreamCAFiles:          splitPaths(upstreamCAFile),
		CallbackCAFiles:          splitPaths(callbackCAFile),
		CallbackPolicy:           callbackpolicy.NewFromCSV(callbackAllowlist),
		DisableSourceIPCheck:     disableSourceIPCheck,
		UserListenAddr:           userAddr,
		AgentServiceHostOverride: agentHostOverride,
		AgentServicePortOverride: int32(agentPortOverride),
		MaxFallbackDepth:         maxFallbackDepth,
		MaxMessageBodyBytes:      maxMessageBodyBytes,
		MaxResponseBodyBytes:     maxResponseBodyBytes,
		SyncDeliveryDeadline:     syncDeliveryDeadline,
		AgentReadTimeout:         agentReadTimeout,
		AgentConnectTimeout:      agentConnectTimeout,
		ChannelHealthWindow:      channelHealthWindow,
		DeliveryBackoff:          parseBackoff(deliveryBackoff, logger),
		CallbackBackoff:          parseBackoff(callbackBackoff, logger),
		DiscordAPIBaseURL:        discordAPIBaseURL,
		WhatsAppAPIBaseURL:       whatsAppAPIBaseURL,
		Replicas: func() int {
			var pods corev1.PodList
			if err := cl.GetClient().List(context.Background(), &pods,
				client.InNamespace(operatorNamespace),
				client.MatchingLabels{"app.kubernetes.io/component": "gateway"}); err != nil || len(pods.Items) == 0 {
				return 1
			}
			return len(pods.Items)
		},
	}, store, tokens, gateway.NewMemorySpend())
	server.Async = async
	server.Completions = &gateway.KubeCompletionWriter{Client: clientset}
	server.Metrics = gateway.NewMetrics(metrics.Registry)
	server.Recorder = cl.GetEventRecorderFor("kaalm-gateway")

	// Prometheus metrics on a dedicated unauthenticated in-cluster port.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
		srv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics listener failed", "error", err)
		}
	}()
	if activatorClient, err := gateway.NewControllerActivator(
		operatorNamespace, certFile, keyFile, caFile); err == nil {
		server.Activator = activatorClient
	} else {
		logger.Info("activator client disabled", "reason", err.Error())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := cl.Start(ctx); err != nil {
			logger.Error("cluster cache failed", "error", err)
			stop()
		}
	}()
	if !cl.GetCache().WaitForCacheSync(ctx) {
		logger.Error("cache sync failed")
		os.Exit(1)
	}

	// The budget counter exchange: publish this replica's partials and fold
	// peers' on a 10s cadence. POD_NAME comes from the downward API.
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName, _ = os.Hostname()
	}
	defer setupTracing(ctx, server, otlpEndpoint, otlpSampleRatio, upstreamCAFile, podName, logger)()
	publisher := &gateway.BudgetPublisher{
		Client: clientset, Ledger: server.Budget,
		OperatorNamespace: operatorNamespace, PodName: podName,
		Providers: func(ctx context.Context) []*kaalmv1beta1.ModelProvider {
			var list kaalmv1beta1.ModelProviderList
			if err := cl.GetClient().List(ctx, &list); err != nil {
				return nil
			}
			out := make([]*kaalmv1beta1.ModelProvider, 0, len(list.Items))
			for i := range list.Items {
				out = append(out, &list.Items[i])
			}
			return out
		},
	}
	publisher.SeedFromCanonical(ctx)
	go publisher.Run(ctx)
	if err := metrics.Registry.Register(&gateway.BudgetUtilizationCollector{
		Ledger: server.Budget, Providers: publisher.Providers,
	}); err != nil {
		logger.Error("registering the budget utilization gauge failed", "error", err)
		os.Exit(1)
	}

	// The watch-driven half of the budget fold: peer partials land in the
	// enforcement view one watch propagation after they are published, which
	// hard enforcement's boundary region depends on (the tick remains the
	// backstop). The cache scopes ConfigMaps to the operator namespace above.
	if informer, err := cl.GetCache().GetInformer(ctx, &corev1.ConfigMap{}); err == nil {
		_, _ = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				gateway.FoldBudgetConfigMapEvent(ctx, obj, podName, store, server.Budget)
			},
			UpdateFunc: func(_, newObj any) {
				gateway.FoldBudgetConfigMapEvent(ctx, newObj, podName, store, server.Budget)
			},
		})
	} else {
		logger.Warn("budget ConfigMap informer unavailable; folds ride the tick only", "error", err)
	}

	// The gateway's half of the channel-delete handshake: once a channel is
	// observed Terminating, confirm disconnection with the annotation the
	// reconciler waits on. The webhook write gate itself lives in the intake
	// handler.
	if informer, err := cl.GetCache().GetInformer(ctx, &kaalmv1beta1.AgentChannel{}); err == nil {
		_, _ = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, newObj any) {
				ch, ok := newObj.(*kaalmv1beta1.AgentChannel)
				if !ok || ch.Status.Phase != kaalmv1beta1.ChannelTerminating {
					return
				}
				if ch.Annotations[kaalmv1beta1.AnnotationChannelDisconnected] == kaalmv1beta1.AnnotationTrue {
					return
				}
				patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`,
					kaalmv1beta1.AnnotationChannelDisconnected, kaalmv1beta1.AnnotationTrue))
				if err := cl.GetClient().Patch(ctx, ch.DeepCopy(),
					client.RawPatch(types.MergePatchType, patch)); err != nil {
					logger.Warn("disconnect annotation patch failed", "channel", ch.Name, "error", err)
				}
			},
		})
	}

	logger.Info("kaalm gateway starting",
		"listen", listenAddr, "health", healthAddr, "operator_namespace", operatorNamespace,
		"source_ip_check_disabled", disableSourceIPCheck)
	if err := server.Run(ctx); err != nil {
		logger.Error("gateway listener failed", "error", err)
		os.Exit(1)
	}
	logger.Info("kaalm gateway shut down")
}

// parseBackoff parses a comma-separated duration schedule like "1s,5s,25s".
// An empty or malformed value falls back to the Config default (nil).
// splitPaths parses a comma-separated file list, dropping blanks so an unset
// flag yields no paths.
func splitPaths(csv string) []string {
	var out []string
	for _, part := range strings.Split(csv, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// validateCABundle checks that the PEM bundle at file exists and parses, and
// exits if not: a misconfigured trust bundle must not be silently ignored. The
// pool itself is built (and rebuilt on rotation) by the server. kind names the
// bundle in log messages; an empty file is a no-op.
func validateCABundle(file, kind string, logger *slog.Logger) {
	if file == "" {
		return
	}
	pem, err := os.ReadFile(file)
	if err != nil {
		logger.Error("reading "+kind+" CA bundle", "error", err)
		os.Exit(1)
	}
	if !x509.NewCertPool().AppendCertsFromPEM(pem) {
		logger.Error("no certificates parsed from " + kind + " CA bundle")
		os.Exit(1)
	}
}

func parseBackoff(raw string, logger *slog.Logger) []time.Duration {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			logger.Warn("ignoring malformed backoff entry", "value", part, "error", err)
			continue
		}
		out = append(out, d)
	}
	return out
}

// setupTracing wires the OTLP exporter when an endpoint is configured and
// returns the shutdown flush; the empty default leaves Server.Tracing nil
// (no tracer installed, no trace context created or forwarded).
func setupTracing(ctx context.Context, server *gateway.Server,
	endpoint string, sampleRatio float64, upstreamCAFile, podName string, logger *slog.Logger,
) func() {
	if endpoint == "" {
		return func() {}
	}
	pool, err := (&tlsutil.CAPoolLoader{Files: splitPaths(upstreamCAFile), Additive: true}).Load()
	if err != nil {
		logger.Error("building the tracing trust pool failed", "error", err)
		os.Exit(1)
	}
	tracing, err := gateway.NewTracing(ctx, endpoint, sampleRatio, pool, podName)
	if err != nil {
		logger.Error("initializing tracing failed", "error", err)
		os.Exit(1)
	}
	server.Tracing = tracing
	logger.Info("tracing enabled", "endpoint", endpoint, "sampleRatio", sampleRatio)
	return func() { _ = tracing.Shutdown(context.Background()) }
}
