package stream

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/user/price-alerter/internal/domain"
)

// Provider описывает внешний источник потоковых котировок (например, WebSocket биржи).
type Provider interface {
	Name() string
	StreamQuotes(ctx context.Context) <-chan domain.Quote
}

// SimulatedWSProvider эмулирует WebSocket-поток биржевых котировок (Binance, Coinbase, Kraken)
// для автономного запуска, нагрузочного тестирования и демонстрации.
type SimulatedWSProvider struct {
	name     string
	interval time.Duration
	base     map[string]float64
}

func NewSimulatedWSProvider(name string, interval time.Duration) *SimulatedWSProvider {
	return &SimulatedWSProvider{
		name:     name,
		interval: interval,
		base: map[string]float64{
			"BTCUSDT": 68000.0,
			"ETHUSDT": 3500.0,
			"SOLUSDT": 175.0,
			"TONUSDT": 6.50,
		},
	}
}

func (p *SimulatedWSProvider) Name() string {
	return p.name
}

func (p *SimulatedWSProvider) StreamQuotes(ctx context.Context) <-chan domain.Quote {
	out := make(chan domain.Quote, 64)
	go func() {
		defer close(out)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		symbols := make([]string, 0, len(p.base))
		prices := make(map[string]float64, len(p.base))
		for k, v := range p.base {
			symbols = append(symbols, k)
			prices[k] = v
		}

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				sym := symbols[rng.Intn(len(symbols))]
				// Случайное колебание цены ±0.4%
				delta := (rng.Float64() - 0.495) * 0.008
				newPrice := prices[sym] * (1.0 + delta)
				prices[sym] = newPrice

				q := domain.Quote{
					Symbol:    sym,
					Price:     newPrice,
					Volume:    rng.Float64() * 15.0,
					Source:    p.name,
					Timestamp: now.UTC(),
				}
				select {
				case out <- q:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// FanIn объединяет несколько входных каналов котировок от разных провайдеров
// в единый агрегированный поток (паттерн Fan-In).
func FanIn(ctx context.Context, channels ...<-chan domain.Quote) <-chan domain.Quote {
	out := make(chan domain.Quote, 256)
	var wg sync.WaitGroup
	wg.Add(len(channels))

	for _, ch := range channels {
		go func(c <-chan domain.Quote) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case q, ok := <-c:
					if !ok {
						return
					}
					select {
					case out <- q:
					case <-ctx.Done():
						return
					}
				}
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Broadcaster реализует паттерн Fan-Out для рассылки сработавших алертов
// и живых котировок подключённым подписчикам (SSE / WebSocket / Webhook воркерам).
type Broadcaster[T any] struct {
	mu      sync.RWMutex
	clients map[int]chan T
	nextID  int
}

func NewBroadcaster[T any]() *Broadcaster[T] {
	return &Broadcaster[T]{
		clients: make(map[int]chan T),
	}
}

func (b *Broadcaster[T]) Subscribe(buffer int) (int, <-chan T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := b.nextID
	ch := make(chan T, buffer)
	b.clients[id] = ch
	return id, ch
}

func (b *Broadcaster[T]) Unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.clients[id]; ok {
		delete(b.clients, id)
		close(ch)
	}
}

// Publish рассылает сообщение всем активным клиентам без блокировки медленными потребителями.
func (b *Broadcaster[T]) Publish(msg T) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.clients {
		select {
		case ch <- msg:
		default:
			// Пропускаем переполненный буфер медленного клиента, чтобы не тормозить hot path
		}
	}
}
