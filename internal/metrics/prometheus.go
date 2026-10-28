package metrics

import (
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

// Collector собирает технические и бизнес-метрики сервиса и отдаёт их
// в текстовом формате Prometheus Exposition Format (0.0.4) без внешних зависимостей.
type Collector struct {
	startTime       time.Time
	httpRequests    atomic.Uint64
	grpcRequests    atomic.Uint64
	wsConnections   atomic.Int64
	quotesIngested  func() (processed, alerts uint64, activeSubs int)
	cacheStats      func() (hits, misses, shared uint64)
}

func NewCollector(
	quotesFn func() (uint64, uint64, int),
	cacheFn func() (uint64, uint64, uint64),
) *Collector {
	return &Collector{
		startTime:      time.Now(),
		quotesIngested: quotesFn,
		cacheStats:     cacheFn,
	}
}

func (c *Collector) IncHTTPRequest() {
	c.httpRequests.Add(1)
}

func (c *Collector) IncGRPCRequest() {
	c.grpcRequests.Add(1)
}

func (c *Collector) AddStreamClient(delta int64) {
	c.wsConnections.Add(delta)
}

func (c *Collector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		processed, alerts, activeSubs := c.quotesIngested()
		hits, misses, shared := c.cacheStats()

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		fmt.Fprintf(w, "# HELP alerter_quotes_processed_total Total number of streaming quotes evaluated by engine.\n")
		fmt.Fprintf(w, "# TYPE alerter_quotes_processed_total counter\n")
		fmt.Fprintf(w, "alerter_quotes_processed_total %d\n\n", processed)

		fmt.Fprintf(w, "# HELP alerter_triggered_alerts_total Total number of price alerts triggered.\n")
		fmt.Fprintf(w, "# TYPE alerter_triggered_alerts_total counter\n")
		fmt.Fprintf(w, "alerter_triggered_alerts_total %d\n\n", alerts)

		fmt.Fprintf(w, "# HELP alerter_active_subscriptions Current number of active price subscriptions.\n")
		fmt.Fprintf(w, "# TYPE alerter_active_subscriptions gauge\n")
		fmt.Fprintf(w, "alerter_active_subscriptions %d\n\n", activeSubs)

		fmt.Fprintf(w, "# HELP alerter_quote_cache_hits_total Total in-memory quote cache hits.\n")
		fmt.Fprintf(w, "# TYPE alerter_quote_cache_hits_total counter\n")
		fmt.Fprintf(w, "alerter_quote_cache_hits_total %d\n\n", hits)

		fmt.Fprintf(w, "# HELP alerter_quote_cache_misses_total Total in-memory quote cache misses.\n")
		fmt.Fprintf(w, "# TYPE alerter_quote_cache_misses_total counter\n")
		fmt.Fprintf(w, "alerter_quote_cache_misses_total %d\n\n", misses)

		fmt.Fprintf(w, "# HELP alerter_singleflight_shared_total Total concurrent requests coalesced via singleflight.\n")
		fmt.Fprintf(w, "# TYPE alerter_singleflight_shared_total counter\n")
		fmt.Fprintf(w, "alerter_singleflight_shared_total %d\n\n", shared)

		fmt.Fprintf(w, "# HELP alerter_http_requests_total Total HTTP API requests.\n")
		fmt.Fprintf(w, "# TYPE alerter_http_requests_total counter\n")
		fmt.Fprintf(w, "alerter_http_requests_total %d\n\n", c.httpRequests.Load())

		fmt.Fprintf(w, "# HELP alerter_stream_clients_active Active real-time stream subscribers.\n")
		fmt.Fprintf(w, "# TYPE alerter_stream_clients_active gauge\n")
		fmt.Fprintf(w, "alerter_stream_clients_active %d\n\n", c.wsConnections.Load())

		fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines that currently exist.\n")
		fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
		fmt.Fprintf(w, "go_goroutines %d\n\n", runtime.NumGoroutine())

		fmt.Fprintf(w, "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_alloc_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_alloc_bytes %d\n", mem.Alloc)
	}
}
