package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/user/price-alerter/internal/domain"
)

func TestMatcher_EvaluateTriggersOnce(t *testing.T) {
	m := NewMatcher(100)

	err := m.AddSubscription(domain.Subscription{
		ID:        "sub-1",
		UserID:    "user-42",
		Symbol:    "BTCUSDT",
		Condition: domain.ConditionAbove,
		Threshold: 70000.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Цена ниже порога — срабатываний нет
	events := m.Evaluate(domain.Quote{
		Symbol:    "BTCUSDT",
		Price:     69500.0,
		Source:    "binance",
		Timestamp: time.Now(),
	})
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}

	// Цена пробила порог — должно сработать ровно 1 раз
	events = m.Evaluate(domain.Quote{
		Symbol:    "BTCUSDT",
		Price:     70150.0,
		Source:    "binance",
		Timestamp: time.Now(),
	})
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].SubscriptionID != "sub-1" || events[0].ActualPrice != 70150.0 {
		t.Errorf("unexpected event payload: %+v", events[0])
	}

	// Повторный тик выше порога — триггер уже погашен
	events = m.Evaluate(domain.Quote{
		Symbol:    "BTCUSDT",
		Price:     70500.0,
		Source:    "binance",
		Timestamp: time.Now(),
	})
	if len(events) != 0 {
		t.Fatalf("expected 0 events on second tick, got %d", len(events))
	}
}

func TestMatcher_ConcurrentRaceSafety(t *testing.T) {
	m := NewMatcher(500)
	symbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "TONUSDT"}

	var wg sync.WaitGroup

	// Конкурентное добавление подписок
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = m.AddSubscription(domain.Subscription{
				ID:        fmt.Sprintf("sub-%d", idx),
				UserID:    fmt.Sprintf("user-%d", idx%10),
				Symbol:    symbols[idx%len(symbols)],
				Condition: domain.ConditionAbove,
				Threshold: float64(100 + idx),
			})
		}(i)
	}

	// Конкурентная оценка входящих котировок
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = m.Evaluate(domain.Quote{
				Symbol:    symbols[idx%len(symbols)],
				Price:     float64(80 + idx),
				Source:    "coinbase",
				Timestamp: time.Now(),
			})
		}(i)
	}

	// Конкурентное чтение подписок и истории
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.ListSubscriptions("")
			_ = m.ListHistory("", 20)
		}()
	}

	wg.Wait()
}

// BenchmarkMatcher_EvaluateHotPath измеряет скорость и количество аллокаций
// на горячем пути проверки котировки (0 allocs/op при отсутствии пробоя порога).
func BenchmarkMatcher_EvaluateHotPath(b *testing.B) {
	m := NewMatcher(1000)
	symbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT", "DOTUSDT"}
	for i := 0; i < 500; i++ {
		_ = m.AddSubscription(domain.Subscription{
			ID:        fmt.Sprintf("sub-%d", i),
			UserID:    "bench-user",
			Symbol:    symbols[i%len(symbols)],
			Condition: domain.ConditionAbove,
			Threshold: 999999.0, // Порог не пробивается на горячем пути
		})
	}

	quote := domain.Quote{
		Symbol:    "BTCUSDT",
		Price:     65000.0,
		Source:    "binance-ws",
		Timestamp: time.Now(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = m.Evaluate(quote)
		}
	})
}
