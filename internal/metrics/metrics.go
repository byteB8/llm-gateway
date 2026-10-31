// Package metrics defines the gateway's Prometheus metrics. They are registered on a registry the
// caller owns, so tests can read them and a process can serve them on its admin port only.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Latency buckets in seconds. TTFT is short when the prefix is cached and long when a request
// queues; durations include the whole streamed response.
var (
	TTFTBuckets     = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	DurationBuckets = []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
)

type Gateway struct {
	Requests        *prometheus.CounterVec   // tenant, backend, code
	TTFT            *prometheus.HistogramVec // backend
	Duration        *prometheus.HistogramVec // backend
	BackendFailures *prometheus.CounterVec   // backend, reason
	RateLimited     *prometheus.CounterVec   // tenant, reason
	Tokens          *prometheus.CounterVec   // tenant, kind (prompt | completion)
	Interrupted     *prometheus.CounterVec   // backend, side (client | backend)
	reg             prometheus.Registerer
}

func NewGateway(reg prometheus.Registerer) *Gateway {
	m := &Gateway{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_requests_total",
			Help: "Chat requests by tenant, serving backend and HTTP status (backend \"none\" if none was reached).",
		}, []string{"tenant", "backend", "code"}),
		TTFT: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llmgw_ttft_seconds",
			Help:    "Time from request arrival to the first response byte from the backend.",
			Buckets: TTFTBuckets,
		}, []string{"backend"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llmgw_request_duration_seconds",
			Help:    "Time from request arrival to the end of the response.",
			Buckets: DurationBuckets,
		}, []string{"backend"}),
		BackendFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_backend_failures_total",
			Help: "Attempts a backend failed before sending any byte (the request moved to the next backend).",
		}, []string{"backend", "reason"}),
		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_rate_limited_total",
			Help: "Requests rejected by a tenant limit.",
		}, []string{"tenant", "reason"}),
		Tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_tokens_total",
			Help: "Tokens reported by backends, by tenant and kind.",
		}, []string{"tenant", "kind"}),
		Interrupted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_stream_interrupted_total",
			Help: "Responses cut after they started: by the client leaving or by the backend failing mid-stream.",
		}, []string{"backend", "side"}),
		reg: reg,
	}
	reg.MustRegister(m.Requests, m.TTFT, m.Duration, m.BackendFailures, m.RateLimited, m.Tokens, m.Interrupted)
	return m
}

// GaugeFunc registers a gauge whose value is read at scrape time, e.g. a backend's in-flight
// count that the router already keeps, so nothing is counted twice.
func (m *Gateway) GaugeFunc(name, help string, labels prometheus.Labels, f func() float64) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, f))
}

// CounterFunc registers a counter read at scrape time from a monotonic source (publisher stats).
func (m *Gateway) CounterFunc(name, help string, labels prometheus.Labels, f func() float64) {
	m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels}, f))
}

type Meter struct {
	Records      *prometheus.CounterVec // result: new | duplicate | malformed
	BatchSeconds prometheus.Histogram
	DBErrors     prometheus.Counter
	CommitErrors prometheus.Counter
	Lag          prometheus.Gauge
}

func NewMeter(reg prometheus.Registerer) *Meter {
	m := &Meter{
		Records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_meter_records_total",
			Help: "Usage records consumed: new (billed), duplicate (replayed, ignored) or malformed (skipped).",
		}, []string{"result"}),
		BatchSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "llmgw_meter_batch_seconds",
			Help:    "Time to write one batch to Postgres, including retries.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 5, 30},
		}),
		DBErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "llmgw_meter_db_errors_total",
			Help: "Failed batch writes (each is retried).",
		}),
		CommitErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "llmgw_meter_commit_errors_total",
			Help: "Failed offset commits (the batch is replayed and ignored).",
		}),
		Lag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "llmgw_meter_consumer_lag",
			Help: "Usage records in Kafka not yet committed by the meter's consumer group, over all partitions.",
		}),
	}
	reg.MustRegister(m.Records, m.BatchSeconds, m.DBErrors, m.CommitErrors, m.Lag)
	return m
}
