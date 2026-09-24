package apiserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the API's RED series: how many requests, how they ended, how
// long they took, and how many subscriptions are open right now.
//
// Rate/errors/duration per verb rather than in aggregate, because the verbs fail
// for unrelated reasons and at unrelated rates — CreateSession waits on
// Kubernetes and a human, Prompt waits on an agent, GetSession waits on SQLite.
// One latency histogram over all of them would describe none of them.
//
// Subscriptions get a gauge of their own: they are the only unbounded resource
// here (a goroutine and a database reader each), and the failure they produce is
// a slow leak rather than an error rate.
type Metrics struct {
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	subscriptions prometheus.Gauge
	registry      *prometheus.Registry
}

// NewMetrics builds the metric set and its registry.
func NewMetrics() *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "harnessapi_requests_total",
			Help: "Harness API requests, by verb and outcome.",
		}, []string{"verb", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "harnessapi_request_duration_seconds",
			Help:    "Harness API request duration, by verb.",
			Buckets: prometheus.DefBuckets,
		}, []string{"verb"}),
		subscriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "harnessapi_subscriptions",
			Help: "Event subscriptions currently open.",
		}),
		registry: prometheus.NewRegistry(),
	}
	m.registry.MustRegister(m.requests, m.duration, m.subscriptions)
	return m
}

// Handler serves the metrics in Prometheus' exposition format. It goes on an
// admin listener, not on the client route: what a deployment measures is not
// something its clients should be able to read.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// interceptor records every call. Streaming calls are counted when they END,
// with the duration of the whole subscription, and are held on the gauge while
// they run — a subscription's interesting number is how long it lasted, not how
// long it took to start.
func (m *Metrics) interceptor() connect.Interceptor {
	return &metricsInterceptor{m: m}
}

type metricsInterceptor struct{ m *Metrics }

func (i *metricsInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		res, err := next(ctx, req)
		i.m.observe(req.Spec().Procedure, start, err)
		return res, err
	}
}

func (i *metricsInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i *metricsInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		start := time.Now()
		i.m.subscriptions.Inc()
		defer i.m.subscriptions.Dec()
		err := next(ctx, conn)
		i.m.observe(conn.Spec().Procedure, start, err)
		return err
	}
}

func (m *Metrics) observe(procedure string, start time.Time, err error) {
	verb := verbOf(procedure)
	m.requests.WithLabelValues(verb, codeOf(err)).Inc()
	m.duration.WithLabelValues(verb).Observe(time.Since(start).Seconds())
}

// verbOf reduces a Connect procedure ("/harnessapi.v1.HarnessAPIService/Prompt")
// to the verb. The package and service are constant here, so keeping them would
// only make every series name longer.
func verbOf(procedure string) string {
	if i := strings.LastIndex(procedure, "/"); i >= 0 {
		return procedure[i+1:]
	}
	return procedure
}

// codeOf labels an outcome. "ok" for success; otherwise the Connect code, which
// is the same classification a client sees — so a spike in the metric and a
// complaint from a client name the same thing.
func codeOf(err error) string {
	if err == nil {
		return "ok"
	}
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		return cerr.Code().String()
	}
	return connect.CodeUnknown.String()
}
