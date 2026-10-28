package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/user/price-alerter/internal/domain"
)

func TestQuoteCache_SingleflightPreventsThunderingHerd(t *testing.T) {
	c := NewQuoteCache(200 * time.Millisecond)
	var loaderCalls atomic.Int32

	loader := func(ctx context.Context, symbol string) (domain.Quote, error) {
		loaderCalls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return domain.Quote{
			Symbol:    symbol,
			Price:     65000.50,
			Source:    "upstream-rest",
			Timestamp: time.Now(),
		}, nil
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			q, err := c.GetOrFetch(context.Background(), "BTCUSDT", loader)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if q.Price != 65000.50 {
				t.Errorf("unexpected price: %f", q.Price)
			}
		}()
	}

	wg.Wait()

	if calls := loaderCalls.Load(); calls != 1 {
		t.Fatalf("expected exactly 1 upstream call due to singleflight, got %d", calls)
	}
}

func BenchmarkQuoteCache_Get(b *testing.B) {
	c := NewQuoteCache(5 * time.Second)
	c.Set(domain.Quote{
		Symbol:    "BTCUSDT",
		Price:     64200.0,
		Source:    "binance-ws",
		Timestamp: time.Now(),
	})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get("BTCUSDT")
		}
	})
}
