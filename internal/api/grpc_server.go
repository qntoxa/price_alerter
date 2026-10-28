package api

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/user/price-alerter/internal/cache"
	"github.com/user/price-alerter/internal/domain"
	"github.com/user/price-alerter/internal/engine"
	"github.com/user/price-alerter/internal/metrics"
)

// RPCServer реализует легковесный JSON-RPC / gRPC-совместимый TCP-транспорт
// для межсервисного взаимодействия без внешних C-зависимостей, сохраняя контракт из api/proto/alerter.proto.
type RPCServer struct {
	addr       string
	matcher    *engine.Matcher
	quoteCache *cache.QuoteCache
	metrics    *metrics.Collector
	logger     *slog.Logger
}

func NewRPCServer(addr string, matcher *engine.Matcher, quoteCache *cache.QuoteCache, collector *metrics.Collector, logger *slog.Logger) *RPCServer {
	return &RPCServer{
		addr:       addr,
		matcher:    matcher,
		quoteCache: quoteCache,
		metrics:    collector,
		logger:     logger,
	}
}

type rpcRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	OK    bool   `json:"ok"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

func (s *RPCServer) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.logger.Info("RPC server listening", "addr", s.addr)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}
		go s.handleConn(conn)
	}
}

func (s *RPCServer) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	enc := json.NewEncoder(conn)

	for scanner.Scan() {
		s.metrics.IncGRPCRequest()
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			_ = enc.Encode(rpcResponse{OK: false, Error: "invalid request frame"})
			continue
		}

		switch req.Method {
		case "GetQuote":
			var p struct {
				Symbol string `json:"symbol"`
			}
			_ = json.Unmarshal(req.Params, &p)
			q, ok := s.quoteCache.Get(strings.ToUpper(p.Symbol))
			if !ok {
				_ = enc.Encode(rpcResponse{OK: false, Error: "quote not found"})
				continue
			}
			_ = enc.Encode(rpcResponse{OK: true, Data: q})

		case "ListSubscriptions":
			var p struct {
				UserID string `json:"user_id"`
			}
			_ = json.Unmarshal(req.Params, &p)
			subs := s.matcher.ListSubscriptions(p.UserID)
			_ = enc.Encode(rpcResponse{OK: true, Data: subs})

		case "CreateSubscription":
			var sub domain.Subscription
			_ = json.Unmarshal(req.Params, &sub)
			if sub.ID == "" {
				sub.ID = "rpc-" + time.Now().Format("150405.000")
			}
			if err := s.matcher.AddSubscription(sub); err != nil {
				_ = enc.Encode(rpcResponse{OK: false, Error: err.Error()})
				continue
			}
			_ = enc.Encode(rpcResponse{OK: true, Data: sub})

		default:
			_ = enc.Encode(rpcResponse{OK: false, Error: "unknown method"})
		}
	}
}
