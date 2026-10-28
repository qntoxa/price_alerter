package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/user/price-alerter/internal/api"
	"github.com/user/price-alerter/internal/cache"
	"github.com/user/price-alerter/internal/domain"
	"github.com/user/price-alerter/internal/engine"
	"github.com/user/price-alerter/internal/metrics"
	"github.com/user/price-alerter/internal/stream"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	httpAddr := envOrDefault("HTTP_ADDR", ":8080")
	grpcAddr := envOrDefault("GRPC_ADDR", ":9090")
	cacheTTLms := envIntOrDefault("QUOTE_CACHE_TTL_MS", 250)
	workerCount := envIntOrDefault("WORKER_COUNT", 4)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	quoteCache := cache.NewQuoteCache(time.Duration(cacheTTLms) * time.Millisecond)
	matcher := engine.NewMatcher(2000)

	// Добавим стартовые демо-подписки для удобного тестирования сразу после запуска
	_ = matcher.AddSubscription(domain.Subscription{
		ID:        "demo-btc-high",
		UserID:    "trader-1",
		Symbol:    "BTCUSDT",
		Condition: domain.ConditionAbove,
		Threshold: 68150.0,
	})
	_ = matcher.AddSubscription(domain.Subscription{
		ID:        "demo-eth-low",
		UserID:    "trader-1",
		Symbol:    "ETHUSDT",
		Condition: domain.ConditionBelow,
		Threshold: 3485.0,
	})

	alertBroadcaster := stream.NewBroadcaster[domain.AlertEvent]()
	quoteBroadcaster := stream.NewBroadcaster[domain.Quote]()
	collector := metrics.NewCollector(matcher.Metrics, quoteCache.Stats)

	// Конкурентный сбор котировок из нескольких WebSocket-источников (паттерн Fan-In)
	providers := []stream.Provider{
		stream.NewSimulatedWSProvider("binance-ws", 40*time.Millisecond),
		stream.NewSimulatedWSProvider("coinbase-ws", 55*time.Millisecond),
		stream.NewSimulatedWSProvider("kraken-ws", 70*time.Millisecond),
	}

	streams := make([]<-chan domain.Quote, 0, len(providers))
	for _, p := range providers {
		streams = append(streams, p.StreamQuotes(ctx))
	}
	mergedQuotes := stream.FanIn(ctx, streams...)

	// Пул воркеров (Worker Pool) для обработки потока котировок и сопоставления с порогами
	var workerWG sync.WaitGroup
	workerWG.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer workerWG.Done()
			for q := range mergedQuotes {
				quoteCache.Set(q)
				quoteBroadcaster.Publish(q)

				events := matcher.Evaluate(q)
				for _, ev := range events {
					logger.Info("price alert triggered",
						"event_id", ev.ID,
						"subscription_id", ev.SubscriptionID,
						"user_id", ev.UserID,
						"symbol", ev.Symbol,
						"condition", ev.Condition,
						"threshold", ev.Threshold,
						"actual_price", ev.ActualPrice,
						"source", ev.Source,
					)
					alertBroadcaster.Publish(ev)
				}
			}
		}(i)
	}

	// Запуск RPC-сервера
	rpcServer := api.NewRPCServer(grpcAddr, matcher, quoteCache, collector, logger)
	go func() {
		if err := rpcServer.Run(ctx); err != nil {
			logger.Error("RPC server error", "err", err)
		}
	}()

	// Запуск HTTP-сервера (REST + SSE + Prometheus + pprof)
	httpSrv := api.NewServer(matcher, quoteCache, alertBroadcaster, quoteBroadcaster, collector)
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           httpSrv.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("HTTP server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down price-alerter gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	workerWG.Wait()
	logger.Info("price-alerter stopped cleanly")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
