package common

import "github.com/shopspring/decimal"

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type ProcessedPayment struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	Processor     string          `json:"processor"`
	Status        string          `json:"status"`
	CreatedAt     string          `json:"createdAt"`
}

type PaymentProcessorRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	RequestedAt   string          `json:"requestedAt"`
}
