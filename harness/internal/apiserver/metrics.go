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

type Metrics struct {
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	subscriptions prometheus.Gauge
	registry      *prometheus.Registry
}

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

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

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

func verbOf(procedure string) string {
	if i := strings.LastIndex(procedure, "/"); i >= 0 {
		return procedure[i+1:]
	}
	return procedure
}

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
