package common

import (
	"os"
	"time"
)

var (
	Whoami string
	Twin   string
)

const (
	CircuitBreakerTimeout = 800 * time.Millisecond
	MaxRetries            = 5
	RetryDelay            = 200 * time.Millisecond
	PaymentAmount         = "19.90"
)

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

