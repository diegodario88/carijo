package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const streamName = "payments_stream"
const groupName = "payment_processors"
const sortedSetKey = "payments:log"

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type PaymentProcessorRequest struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type PaymentLogEntry struct {
	Processor string  `json:"processor"`
	Amount    float64 `json:"amount"`
}

type Worker struct {
	db                   *redis.Client
	consumer             string
	processorDefaultURL  string
	processorFallbackURL string
	httpClient           *http.Client
}

func NewPaymentWorker(db *redis.Client) *Worker {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return &Worker{
		db:       db,
		consumer: hostname,
		processorDefaultURL: getEnv(
			"PROCESSOR_DEFAULT_URL",
			"http://payment-processor-default:8080/payments",
		),
		processorFallbackURL: getEnv(
			"PROCESSOR_FALLBACK_URL",
			"http://payment-processor-fallback:8080/payments",
		),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.createConsumerGroup(ctx)
	log.Printf(
		"Worker '%s' iniciando, lendo da stream '%s' no grupo '%s'",
		w.consumer,
		streamName,
		groupName,
	)
	for {
		select {
		case <-ctx.Done():
			log.Println("Worker recebendo sinal de desligamento...")
			return nil
		default:
			err := w.readAndProcessMessage(ctx)
			if err != nil && !errors.Is(err, redis.Nil) {
				log.Printf("Erro ao processar mensagem: %v. Aguardando...", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (w *Worker) createConsumerGroup(ctx context.Context) {
	err := w.db.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Erro inesperado ao criar consumer group: %v", err)
	}
}

func (w *Worker) readAndProcessMessage(ctx context.Context) error {
	streams, err := w.db.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: groupName, Consumer: w.consumer,
		Streams: []string{streamName, ">"},
		Count:   1, Block: 2 * time.Second,
	}).Result()
	if err != nil {
		return err
	}

	for _, stream := range streams {
		for _, message := range stream.Messages {
			payload := message.Values["payload"].(string)
			var req PaymentRequest
			if err := json.Unmarshal([]byte(payload), &req); err != nil {
				log.Printf("Erro ao decodificar payload: %v. Mensagem: %s", err, payload)
				w.db.XAck(ctx, streamName, groupName, message.ID)
				continue
			}

			log.Printf("Worker '%s' recebeu pagamento %s", w.consumer, req.CorrelationID)
			processorUsed, err := w.processPayment(ctx, req)
			if err != nil {
				log.Printf("ERRO CRÍTICO: Falha ao processar %s: %v", req.CorrelationID, err)
				w.db.XAck(ctx, streamName, groupName, message.ID)
				continue
			}

			logEntry := PaymentLogEntry{Processor: processorUsed, Amount: req.Amount}
			logEntryJSON, _ := json.Marshal(logEntry)

			member := redis.Z{
				Score:  float64(time.Now().UnixMilli()),
				Member: logEntryJSON,
			}

			if err := w.db.ZAdd(ctx, sortedSetKey, member).Err(); err != nil {
				log.Printf("ERRO: Falha ao salvar log do pagamento %s: %v", req.CorrelationID, err)
				return err // Não dá ACK, permite reprocessamento.
			}

			w.db.XAck(ctx, streamName, groupName, message.ID)
			log.Printf(
				"Pagamento %s processado via '%s' e log salvo.",
				req.CorrelationID,
				processorUsed,
			)
		}
	}
	return nil
}

func (w *Worker) processPayment(ctx context.Context, req PaymentRequest) (string, error) {
	processorReq := PaymentProcessorRequest{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		RequestedAt:   time.Now().UTC(),
	}

	err := w.callPaymentProcessor(ctx, w.processorDefaultURL, processorReq)
	if err == nil {
		return "default", nil
	}
	log.Printf("AVISO: Falha no Default para %s: %v. Tentando Fallback...", req.CorrelationID, err)

	err = w.callPaymentProcessor(ctx, w.processorFallbackURL, processorReq)
	if err == nil {
		return "fallback", nil
	}
	log.Printf("ERRO: Falha no Fallback para %s: %v.", req.CorrelationID, err)

	return "", fmt.Errorf("ambos os processadores falharam")
}

func (w *Worker) callPaymentProcessor(
	ctx context.Context,
	url string,
	req PaymentProcessorRequest,
) error {
	payloadBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("falha no marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("falha ao criar http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("falha na chamada http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status de erro: %d", resp.StatusCode)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

