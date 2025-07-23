package common

import (
	"os"
	"time"
)

var (
	Whoami string
	Twin   string
)

const StreamName = "payments_stream"
const GroupName = "payment_processors"
const PaymentsHashKey = "payments"
const MinIdleDuration = 2 * time.Second
const ClaimUnprocessedCount = 100
const CircuitBreakerTimeout = 1 * time.Second

func init() {
	Whoami, _ = os.Hostname()

	if Whoami == "white-carijo" {
		Twin = "black-carijo"
	} else {
		Twin = "white-carijo"
	}
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

