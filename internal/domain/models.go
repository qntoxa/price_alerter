package domain

import (
	"errors"
	"strings"
	"time"
)

type Condition string

const (
	ConditionAbove Condition = "GT" // Цена выше порога (Greater Than)
	ConditionBelow Condition = "LT" // Цена ниже порога (Less Than)
)

var (
	ErrInvalidSymbol    = errors.New("symbol is required")
	ErrInvalidCondition = errors.New("condition must be GT or LT")
	ErrInvalidThreshold = errors.New("threshold must be positive")
	ErrNotFound         = errors.New("resource not found")
)

// Quote представляет котировку актива, полученную от поставщика данных.
type Quote struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    float64   `json:"volume"`
	Source    string    `json:"source"`
	Timestamp time.Time `json:"timestamp"`
}

// Subscription описывает пользовательское правило срабатывания ценового алерта.
type Subscription struct {
	ID          string     `json:"id"`
	UserID      string     `json:"user_id"`
	Symbol      string     `json:"symbol"`
	Condition   Condition  `json:"condition"`
	Threshold   float64    `json:"threshold"`
	Active      bool       `json:"active"`
	CreatedAt   time.Time  `json:"created_at"`
	TriggeredAt *time.Time `json:"triggered_at,omitempty"`
}

func (s *Subscription) NormalizeAndValidate() error {
	s.Symbol = strings.ToUpper(strings.TrimSpace(s.Symbol))
	s.Condition = Condition(strings.ToUpper(strings.TrimSpace(string(s.Condition))))
	if s.Symbol == "" {
		return ErrInvalidSymbol
	}
	if s.Condition != ConditionAbove && s.Condition != ConditionBelow {
		return ErrInvalidCondition
	}
	if s.Threshold <= 0 {
		return ErrInvalidThreshold
	}
	return nil
}

// Matches проверяет, достигла ли котировка заданного порога.
func (s *Subscription) Matches(price float64) bool {
	if !s.Active {
		return false
	}
	switch s.Condition {
	case ConditionAbove:
		return price >= s.Threshold
	case ConditionBelow:
		return price <= s.Threshold
	default:
		return false
	}
}

// AlertEvent фиксирует факт срабатывания триггера.
type AlertEvent struct {
	ID             string    `json:"id"`
	SubscriptionID string    `json:"subscription_id"`
	UserID         string    `json:"user_id"`
	Symbol         string    `json:"symbol"`
	Condition      Condition `json:"condition"`
	Threshold      float64   `json:"threshold"`
	ActualPrice    float64   `json:"actual_price"`
	Source         string    `json:"source"`
	TriggeredAt    time.Time `json:"triggered_at"`
}
