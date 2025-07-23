package pkg

import (
	"testing"
	"time"
)

func TestInMemorySummaryStore_Save(t *testing.T) {
	store := NewInMemorySummaryStore()

	timestamp := time.Now()
	store.Save("default", timestamp)
	store.Save("fallback", timestamp)

	summary := store.GetSummary(time.Time{}, time.Time{})

	if summary.DefaultCount != 1 {
		t.Errorf("Expected DefaultCount to be 1, got %d", summary.DefaultCount)
	}

	if summary.FallbackCount != 1 {
		t.Errorf("Expected FallbackCount to be 1, got %d", summary.FallbackCount)
	}
}

func TestInMemorySummaryStore_GetSummaryWithTimeFilter(t *testing.T) {
	store := NewInMemorySummaryStore()

	now := time.Now()
	past := now.Add(-2 * time.Hour)
	future := now.Add(2 * time.Hour)

	store.Save("default", past)
	store.Save("default", now)
	store.Save("fallback", future)

	summary := store.GetSummary(past.Add(-time.Hour), now.Add(time.Hour))

	if summary.DefaultCount != 2 {
		t.Errorf("Expected DefaultCount to be 2, got %d", summary.DefaultCount)
	}

	if summary.FallbackCount != 0 {
		t.Errorf("Expected FallbackCount to be 0, got %d", summary.FallbackCount)
	}
}

func TestInMemorySummaryStore_Purge(t *testing.T) {
	store := NewInMemorySummaryStore()

	timestamp := time.Now()
	store.Save("default", timestamp)
	store.Save("fallback", timestamp)

	summary := store.GetSummary(time.Time{}, time.Time{})
	if summary.DefaultCount != 1 || summary.FallbackCount != 1 {
		t.Fatalf("Initial data not saved correctly. Default: %d, Fallback: %d",
			summary.DefaultCount, summary.FallbackCount)
	}

	store.Purge()

	summary = store.GetSummary(time.Time{}, time.Time{})
	if summary.DefaultCount != 0 {
		t.Errorf("After purge, expected DefaultCount to be 0, got %d", summary.DefaultCount)
	}

	if summary.FallbackCount != 0 {
		t.Errorf("After purge, expected FallbackCount to be 0, got %d", summary.FallbackCount)
	}
}

func TestInMemorySummaryStore_ConcurrentAccess(t *testing.T) {
	store := NewInMemorySummaryStore()

	done := make(chan bool, 2)

	go func() {
		for range 100 {
			store.Save("default", time.Now())
		}
		done <- true
	}()

	go func() {
		for range 100 {
			store.GetSummary(time.Time{}, time.Time{})
		}
		done <- true
	}()

	<-done
	<-done

	summary := store.GetSummary(time.Time{}, time.Time{})
	if summary.DefaultCount != 100 {
		t.Errorf("Expected DefaultCount to be 100, got %d", summary.DefaultCount)
	}
}

func TestCircuitBreakerStore_Allow(t *testing.T) {
	store := NewCircuitBreakerStore()

	if !store.Allow("default") {
		t.Error("Expected circuit breaker to allow initial request")
	}

	if !store.Allow("fallback") {
		t.Error("Expected circuit breaker to allow initial request for fallback")
	}
}

func TestCircuitBreakerStore_RecordFailure(t *testing.T) {
	store := NewCircuitBreakerStore()

	for range 5 {
		store.RecordFailure("test-service")
	}

	if store.Allow("test-service") {
		t.Error("Expected circuit breaker to be open after multiple failures")
	}
}

func TestCircuitBreakerStore_RecordSuccess(t *testing.T) {
	store := NewCircuitBreakerStore()

	store.RecordSuccess("test-service")

	if !store.Allow("test-service") {
		t.Error("Expected circuit breaker to allow requests after success")
	}
}

func TestCircuitBreakerStore_GetAllStatuses(t *testing.T) {
	store := NewCircuitBreakerStore()

	statuses := store.GetAllStatuses()

	if len(statuses) != 2 {
		t.Errorf("Expected 2 circuit breakers, got %d", len(statuses))
	}

	if _, exists := statuses["default"]; !exists {
		t.Error("Expected 'default' circuit breaker to exist")
	}

	if _, exists := statuses["fallback"]; !exists {
		t.Error("Expected 'fallback' circuit breaker to exist")
	}
}

func TestCircuitBreakerStore_GetOrCreate(t *testing.T) {
	store := NewCircuitBreakerStore()

	cb1 := store.getOrCreate("new-service")
	if cb1 == nil {
		t.Error("Expected circuit breaker to be created")
	}

	cb2 := store.getOrCreate("new-service")
	if cb1 != cb2 {
		t.Error("Expected same circuit breaker instance to be returned")
	}
}
