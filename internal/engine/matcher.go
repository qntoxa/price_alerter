package engine

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/user/price-alerter/internal/domain"
)

const shardCount = 16

type symbolShard struct {
	mu   sync.RWMutex
	// map[symbol] -> slice of active subscriptions
	subs map[string][]domain.Subscription
}

// Matcher — высокопроизводительный in-memory движок сопоставления цен с подписками.
// Использует шардирование по хешу тикера для минимизации lock contention и
// оптимизирован для минимального числа аллокаций на горячем пути (hot path).
type Matcher struct {
	shards        [shardCount]symbolShard
	byIDMu        sync.RWMutex
	byID          map[string]domain.Subscription
	historyMu     sync.RWMutex
	history       []domain.AlertEvent
	maxHistory    int
	eventSeq      atomic.Uint64
	processedQuotes atomic.Uint64
	triggeredAlerts atomic.Uint64
}

func NewMatcher(maxHistory int) *Matcher {
	if maxHistory <= 0 {
		maxHistory = 1000
	}
	m := &Matcher{
		byID:       make(map[string]domain.Subscription, 256),
		history:    make([]domain.AlertEvent, 0, maxHistory),
		maxHistory: maxHistory,
	}
	for i := 0; i < shardCount; i++ {
		m.shards[i].subs = make(map[string][]domain.Subscription, 16)
	}
	return m
}

// fnv32a вычисляет быстрый хеш строки без аллокаций в куче.
func fnv32a(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (m *Matcher) getShard(symbol string) *symbolShard {
	idx := fnv32a(symbol) & (shardCount - 1)
	return &m.shards[idx]
}

// AddSubscription регистрирует новую подписку в in-memory индексе.
func (m *Matcher) AddSubscription(sub domain.Subscription) error {
	if err := sub.NormalizeAndValidate(); err != nil {
		return err
	}
	sub.Active = true
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now().UTC()
	}

	shard := m.getShard(sub.Symbol)
	shard.mu.Lock()
	shard.subs[sub.Symbol] = append(shard.subs[sub.Symbol], sub)
	shard.mu.Unlock()

	m.byIDMu.Lock()
	m.byID[sub.ID] = sub
	m.byIDMu.Unlock()

	return nil
}

// RemoveSubscription удаляет подписку по её ID.
func (m *Matcher) RemoveSubscription(id string) bool {
	m.byIDMu.Lock()
	sub, ok := m.byID[id]
	if !ok {
		m.byIDMu.Unlock()
		return false
	}
	delete(m.byID, id)
	m.byIDMu.Unlock()

	shard := m.getShard(sub.Symbol)
	shard.mu.Lock()
	list := shard.subs[sub.Symbol]
	for i := range list {
		if list[i].ID == id {
			list[i] = list[len(list)-1]
			shard.subs[sub.Symbol] = list[:len(list)-1]
			break
		}
	}
	shard.mu.Unlock()
	return true
}

// Evaluate проверяет котировку против активных подписок по данному тикеру.
// При срабатывании атомарно деактивирует одноразовый триггер (one-shot alert)
// и возвращает список сработавших событий.
func (m *Matcher) Evaluate(q domain.Quote) []domain.AlertEvent {
	m.processedQuotes.Add(1)
	sym := strings.ToUpper(q.Symbol)
	shard := m.getShard(sym)

	// Сначала быстрый RLock на горячем пути: если совпадений нет — не берём эксклюзивную блокировку
	shard.mu.RLock()
	list := shard.subs[sym]
	if len(list) == 0 {
		shard.mu.RUnlock()
		return nil
	}
	hasMatch := false
	for i := range list {
		if list[i].Matches(q.Price) {
			hasMatch = true
			break
		}
	}
	shard.mu.RUnlock()

	if !hasMatch {
		return nil
	}

	// Если найден хотя бы один сработавший триггер, берём Lock и удаляем сработавшие подписки in-place
	now := time.Now().UTC()
	var events []domain.AlertEvent

	shard.mu.Lock()
	list = shard.subs[sym]
	n := 0
	for i := range list {
		sub := list[i]
		if sub.Matches(q.Price) {
			seq := m.eventSeq.Add(1)
			ev := domain.AlertEvent{
				ID:             fmt.Sprintf("evt-%d", seq),
				SubscriptionID: sub.ID,
				UserID:         sub.UserID,
				Symbol:         sub.Symbol,
				Condition:      sub.Condition,
				Threshold:      sub.Threshold,
				ActualPrice:    q.Price,
				Source:         q.Source,
				TriggeredAt:    now,
			}
			events = append(events, ev)
		} else {
			list[n] = sub
			n++
		}
	}
	shard.subs[sym] = list[:n]
	shard.mu.Unlock()

	if len(events) > 0 {
		m.triggeredAlerts.Add(uint64(len(events)))

		m.byIDMu.Lock()
		for _, ev := range events {
			if s, ok := m.byID[ev.SubscriptionID]; ok {
				s.Active = false
				t := ev.TriggeredAt
				s.TriggeredAt = &t
				m.byID[ev.SubscriptionID] = s
			}
		}
		m.byIDMu.Unlock()

		m.historyMu.Lock()
		m.history = append(m.history, events...)
		if len(m.history) > m.maxHistory {
			m.history = m.history[len(m.history)-m.maxHistory:]
		}
		m.historyMu.Unlock()
	}

	return events
}

func (m *Matcher) ListSubscriptions(userID string) []domain.Subscription {
	m.byIDMu.RLock()
	defer m.byIDMu.RUnlock()

	res := make([]domain.Subscription, 0, len(m.byID))
	for _, sub := range m.byID {
		if userID == "" || sub.UserID == userID {
			res = append(res, sub)
		}
	}
	return res
}

func (m *Matcher) ListHistory(userID string, limit int) []domain.AlertEvent {
	m.historyMu.RLock()
	defer m.historyMu.RUnlock()

	if limit <= 0 || limit > len(m.history) {
		limit = len(m.history)
	}
	res := make([]domain.AlertEvent, 0, limit)
	// Возвращаем от новых к старым
	for i := len(m.history) - 1; i >= 0 && len(res) < limit; i-- {
		ev := m.history[i]
		if userID == "" || ev.UserID == userID {
			res = append(res, ev)
		}
	}
	return res
}

func (m *Matcher) Metrics() (processedQuotes, triggeredAlerts uint64, activeSubs int) {
	m.byIDMu.RLock()
	for _, s := range m.byID {
		if s.Active {
			activeSubs++
		}
	}
	m.byIDMu.RUnlock()
	return m.processedQuotes.Load(), m.triggeredAlerts.Load(), activeSubs
}
