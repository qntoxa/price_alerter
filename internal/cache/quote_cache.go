package cache

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/user/price-alerter/internal/domain"
)

type cacheEntry struct {
	quote     domain.Quote
	expiresAt int64 // UnixNano
}

// QuoteCache хранит актуальные котировки в памяти с поддержкой TTL и singleflight-защитой
// от лавинообразных запросов к внешнему источнику при промахе кеша.
type QuoteCache struct {
	mu          sync.RWMutex
	items       map[string]cacheEntry
	ttl         time.Duration
	sf          *Group[domain.Quote]
	hits        atomic.Uint64
	misses      atomic.Uint64
	sfShared    atomic.Uint64
}

func NewQuoteCache(ttl time.Duration) *QuoteCache {
	return &QuoteCache{
		items: make(map[string]cacheEntry, 64),
		ttl:   ttl,
		sf:    NewGroup[domain.Quote](),
	}
}

func (c *QuoteCache) Set(q domain.Quote) {
	sym := strings.ToUpper(q.Symbol)
	exp := time.Now().Add(c.ttl).UnixNano()

	c.mu.Lock()
	c.items[sym] = cacheEntry{
		quote:     q,
		expiresAt: exp,
	}
	c.mu.Unlock()
}

func (c *QuoteCache) Get(symbol string) (domain.Quote, bool) {
	sym := strings.ToUpper(symbol)
	now := time.Now().UnixNano()

	c.mu.RLock()
	entry, ok := c.items[sym]
	c.mu.RUnlock()

	if !ok || now > entry.expiresAt {
		c.misses.Add(1)
		return domain.Quote{}, false
	}
	c.hits.Add(1)
	return entry.quote, true
}

// GetOrFetch возвращает котировку из кеша или вызывает loader ровно один раз
// для всех конкурентных запросов по одному тикеру с помощью singleflight.
func (c *QuoteCache) GetOrFetch(ctx context.Context, symbol string, loader func(context.Context, string) (domain.Quote, error)) (domain.Quote, error) {
	sym := strings.ToUpper(symbol)
	if q, ok := c.Get(sym); ok {
		return q, nil
	}

	q, err, shared := c.sf.Do(sym, func() (domain.Quote, error) {
		// Повторная проверка после входа в singleflight
		if cached, ok := c.Get(sym); ok {
			return cached, nil
		}
		fetched, err := loader(ctx, sym)
		if err != nil {
			return domain.Quote{}, err
		}
		c.Set(fetched)
		return fetched, nil
	})
	if shared {
		c.sfShared.Add(1)
	}
	return q, err
}

func (c *QuoteCache) Snapshot() []domain.Quote {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]domain.Quote, 0, len(c.items))
	for _, v := range c.items {
		res = append(res, v.quote)
	}
	return res
}

func (c *QuoteCache) Stats() (hits, misses, shared uint64) {
	return c.hits.Load(), c.misses.Load(), c.sfShared.Load()
}
