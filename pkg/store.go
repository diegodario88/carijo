package pkg

import (
	"sync"
	"time"

	"github.com/diegodario88/carijo/pkg/common"
)

type Payment struct {
	Processor string
	Timestamp time.Time
}

type InMemorySummaryStore struct {
	mu       sync.RWMutex
	payments []Payment
}

type CircuitBreakerStore struct {
	mu       sync.RWMutex
	circuits map[string]*CircuitBreaker
}

type SummaryResult struct {
	DefaultCount  int64
	FallbackCount int64
}

func NewInMemorySummaryStore() *InMemorySummaryStore {
	return &InMemorySummaryStore{
		payments: make([]Payment, 0, 50000),
	}
}

func (s *InMemorySummaryStore) Save(processor string, timestamp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.payments = append(s.payments, Payment{
		Processor: processor,
		Timestamp: timestamp,
	})
}

func (s *InMemorySummaryStore) GetSummary(from, to time.Time) SummaryResult {
	s.mu.RLock()
	snapshot := make([]Payment, len(s.payments))
	copy(snapshot, s.payments)
	s.mu.RUnlock()

	var result SummaryResult
	useFilter := !from.IsZero() && !to.IsZero()

	for _, p := range snapshot {
		if useFilter {
			if p.Timestamp.Before(from) || p.Timestamp.After(to) {
				continue
			}
		}

		switch p.Processor {
		case "default":
			result.DefaultCount++
		case "fallback":
			result.FallbackCount++
		}
	}

	return result
}

func (s *InMemorySummaryStore) Purge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payments = make([]Payment, 0, 50000)
}

func NewCircuitBreakerStore() *CircuitBreakerStore {
	failureThreshold := int64(3)
	successThreshold := int64(2)
	timeout := common.CircuitBreakerTimeout

	store := &CircuitBreakerStore{
		circuits: make(map[string]*CircuitBreaker),
	}

	store.circuits["default"] = NewCircuitBreaker(failureThreshold, successThreshold, timeout)
	store.circuits["fallback"] = NewCircuitBreaker(failureThreshold, successThreshold, timeout)

	return store
}

func (s *CircuitBreakerStore) Allow(name string) bool {
	return s.getOrCreate(name).Allow()
}

func (s *CircuitBreakerStore) RecordFailure(name string) {
	s.getOrCreate(name).RecordFailure()
}

func (s *CircuitBreakerStore) RecordSuccess(name string) {
	s.getOrCreate(name).RecordSuccess()
}

func (s *CircuitBreakerStore) GetAllStatuses() map[string]Status {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statuses := make(map[string]Status, len(s.circuits))
	for name, cb := range s.circuits {
		statuses[name] = cb.GetStatus()
	}
	return statuses
}

func (s *CircuitBreakerStore) getOrCreate(name string) *CircuitBreaker {
	s.mu.RLock()
	cb, ok := s.circuits[name]
	s.mu.RUnlock()

	if ok {
		return cb
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cb, ok = s.circuits[name]
	if ok {
		return cb
	}

	failureThreshold := int64(3)
	successThreshold := int64(2)
	timeout := common.CircuitBreakerTimeout
	cb = NewCircuitBreaker(failureThreshold, successThreshold, timeout)
	s.circuits[name] = cb
	return cb
}

