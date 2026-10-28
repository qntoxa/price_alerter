package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/user/price-alerter/internal/cache"
	"github.com/user/price-alerter/internal/domain"
	"github.com/user/price-alerter/internal/engine"
	"github.com/user/price-alerter/internal/metrics"
	"github.com/user/price-alerter/internal/stream"
)

type Server struct {
	matcher     *engine.Matcher
	quoteCache  *cache.QuoteCache
	alertStream *stream.Broadcaster[domain.AlertEvent]
	quoteStream *stream.Broadcaster[domain.Quote]
	metrics     *metrics.Collector
	subSeq      atomic.Uint64
}

func NewServer(
	matcher *engine.Matcher,
	quoteCache *cache.QuoteCache,
	alertStream *stream.Broadcaster[domain.AlertEvent],
	quoteStream *stream.Broadcaster[domain.Quote],
	collector *metrics.Collector,
) *Server {
	return &Server{
		matcher:     matcher,
		quoteCache:  quoteCache,
		alertStream: alertStream,
		quoteStream: quoteStream,
		metrics:     collector,
	}
}

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	// Health & Observability
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.metrics.Handler())

	// Профилирование pprof (CPU, Heap, Goroutines, Block, Mutex)
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	// Бизнес-эндпоинты REST API
	mux.HandleFunc("POST /api/v1/subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("GET /api/v1/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("DELETE /api/v1/subscriptions/{id}", s.handleDeleteSubscription)
	mux.HandleFunc("GET /api/v1/alerts/history", s.handleListAlertHistory)
	mux.HandleFunc("GET /api/v1/quotes", s.handleListQuotes)
	mux.HandleFunc("GET /api/v1/quotes/{symbol}", s.handleGetQuote)
	mux.HandleFunc("POST /api/v1/quotes/ingest", s.handleIngestQuote)

	// Потоковая передача сработавших алертов в реальном времени (SSE / Stream)
	mux.HandleFunc("GET /api/v1/stream/alerts", s.handleStreamAlerts)

	return s.withTelemetry(mux)
}

func (s *Server) withTelemetry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.metrics.IncHTTPRequest()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	processed, alerts, active := s.matcher.Metrics()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":               "ok",
		"processed_quotes":     processed,
		"triggered_alerts":     alerts,
		"active_subscriptions": active,
		"timestamp":            time.Now().UTC(),
	})
}

type createSubscriptionReq struct {
	UserID    string  `json:"user_id"`
	Symbol    string  `json:"symbol"`
	Condition string  `json:"condition"`
	Threshold float64 `json:"threshold"`
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubscriptionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	if strings.TrimSpace(req.UserID) == "" {
		req.UserID = "anonymous"
	}

	id := fmt.Sprintf("sub-%d", s.subSeq.Add(1))
	sub := domain.Subscription{
		ID:        id,
		UserID:    req.UserID,
		Symbol:    req.Symbol,
		Condition: domain.Condition(req.Condition),
		Threshold: req.Threshold,
		Active:    true,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.matcher.AddSubscription(sub); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, sub)
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	subs := s.matcher.ListSubscriptions(userID)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": subs,
		"count": len(subs),
	})
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.matcher.RemoveSubscription(id) {
		writeError(w, http.StatusNotFound, "subscription not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": id})
}

func (s *Server) handleListAlertHistory(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	limit := 50
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	events := s.matcher.ListHistory(userID, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": events,
		"count": len(events),
	})
}

func (s *Server) handleListQuotes(w http.ResponseWriter, r *http.Request) {
	quotes := s.quoteCache.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"items": quotes,
		"count": len(quotes),
	})
}

func (s *Server) handleGetQuote(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.PathValue("symbol"))
	q, err := s.quoteCache.GetOrFetch(r.Context(), symbol, func(ctx context.Context, sym string) (domain.Quote, error) {
		return domain.Quote{}, domain.ErrNotFound
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "quote not found in cache")
		return
	}
	writeJSON(w, http.StatusOK, q)
}

func (s *Server) handleIngestQuote(w http.ResponseWriter, r *http.Request) {
	var q domain.Quote
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		writeError(w, http.StatusBadRequest, "invalid quote json")
		return
	}
	if q.Timestamp.IsZero() {
		q.Timestamp = time.Now().UTC()
	}
	if q.Source == "" {
		q.Source = "manual-http"
	}
	s.quoteCache.Set(q)
	events := s.matcher.Evaluate(q)
	for _, ev := range events {
		s.alertStream.Publish(ev)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"triggered_count": len(events),
		"events":          events,
	})
}

func (s *Server) handleStreamAlerts(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	s.metrics.AddStreamClient(1)
	defer s.metrics.AddStreamClient(-1)

	subID, ch := s.alertStream.Subscribe(32)
	defer s.alertStream.Unsubscribe(subID)

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: alert\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
