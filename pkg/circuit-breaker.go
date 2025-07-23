package pkg

import (
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type CircuitBreaker struct {
	state                State
	consecutiveFailures  int64
	consecutiveSuccesses int64
	lastFailureTime      int64

	failureThreshold int64
	successThreshold int64
	timeout          time.Duration

	mu sync.Mutex
}

func NewCircuitBreaker(
	failureThreshold int64,
	successThreshold int64,
	timeout time.Duration,
) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	state := atomic.LoadInt32((*int32)(&cb.state))

	if State(state) == StateOpen {
		lastFailureNano := atomic.LoadInt64(&cb.lastFailureTime)
		if time.Now().UnixNano()-lastFailureNano > cb.timeout.Nanoseconds() {
			if atomic.CompareAndSwapInt32((*int32)(&cb.state), state, int32(StateHalfOpen)) {
				atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
				return true
			}
		}
		return false
	}

	return true
}

func (cb *CircuitBreaker) RecordFailure() {
	state := atomic.LoadInt32((*int32)(&cb.state))

	if State(state) == StateHalfOpen {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		if cb.state == StateHalfOpen {
			cb.openCircuit()
		}
		return
	}

	failures := atomic.AddInt64(&cb.consecutiveFailures, 1)
	if failures >= cb.failureThreshold {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		if cb.state == StateClosed {
			cb.openCircuit()
		}
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	state := atomic.LoadInt32((*int32)(&cb.state))

	if State(state) == StateHalfOpen {
		successes := atomic.AddInt64(&cb.consecutiveSuccesses, 1)
		if successes >= cb.successThreshold {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			if cb.state == StateHalfOpen {
				cb.closeCircuit()
			}
		}
		return
	}

	if atomic.LoadInt64(&cb.consecutiveFailures) > 0 {
		atomic.StoreInt64(&cb.consecutiveFailures, 0)
	}
}

func (cb *CircuitBreaker) openCircuit() {
	atomic.StoreInt32((*int32)(&cb.state), int32(StateOpen))
	atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())
}

func (cb *CircuitBreaker) closeCircuit() {
	atomic.StoreInt32((*int32)(&cb.state), int32(StateClosed))
	atomic.StoreInt64(&cb.consecutiveFailures, 0)
}

type Status struct {
	State               string    `json:"state"`
	ConsecutiveFailures int64     `json:"consecutive_failures"`
	LastFailureTime     time.Time `json:"last_failure_time"`
}

func (cb *CircuitBreaker) GetStatus() Status {
	state := atomic.LoadInt32((*int32)(&cb.state))
	failures := atomic.LoadInt64(&cb.consecutiveFailures)
	lastFailureNano := atomic.LoadInt64(&cb.lastFailureTime)

	var lastFailureTime time.Time
	if lastFailureNano > 0 {
		lastFailureTime = time.Unix(0, lastFailureNano)
	}

	return Status{
		State:               State(state).String(),
		ConsecutiveFailures: failures,
		LastFailureTime:     lastFailureTime,
	}
}
