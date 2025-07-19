package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/diegodario88/carijo/pkg/utils"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const streamName = "payments_stream"
const groupName = "payment_processors"
const sortedSetKey = "payments:log"
const dlqKey = "dead_letter_queue"

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentProcessorRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	RequestedAt   string          `json:"requestedAt"`
}

type PaymentLogEntry struct {
	CorrelationID string          `json:"correlationId"`
	Processor     string          `json:"processor"`
	Amount        decimal.Decimal `json:"amount"`
}

type Worker struct {
	Consumer             string
	db                   *redis.Client
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
		Consumer: hostname,
		db:       db,
		processorDefaultURL: utils.GetEnv(
			"PROCESSOR_DEFAULT_URL",
			"http://payment-processor-default:8080/payments",
		),
		processorFallbackURL: utils.GetEnv(
			"PROCESSOR_FALLBACK_URL",
			"http://payment-processor-fallback:8080/payments",
		),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        30,
				MaxIdleConnsPerHost: 15,
				IdleConnTimeout:     120 * time.Second,
				MaxConnsPerHost:     20,
				DisableCompression:  true,
				DisableKeepAlives:   false,
				ForceAttemptHTTP2:   false,

				DialContext: (&net.Dialer{
					Timeout:   2 * time.Second,
					KeepAlive: 30 * time.Second,
					DualStack: true,
				}).DialContext,
			},
		},
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.createConsumerGroup(ctx)
	log.Printf(
		"Worker '%s' iniciando, lendo da stream '%s' no grupo '%s'",
		w.Consumer,
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
		Group:    groupName,
		Consumer: w.Consumer,
		Streams:  []string{streamName, ">"},
		Count:    1, // Processamento sequencial
		Block:    2 * time.Second,
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

			processorUsed, err := w.processPayment(ctx, req)
			if err != nil {
				log.Printf(
					"ERRO CRÍTICO: Falha ao processar %s. Movendo para DLQ.",
					req.CorrelationID,
				)
				w.db.LPush(ctx, dlqKey, payload)
				w.db.XAck(ctx, streamName, groupName, message.ID)
				continue
			}

			logEntry := PaymentLogEntry{
				CorrelationID: req.CorrelationID,
				Processor:     processorUsed,
				Amount:        req.Amount,
			}
			logEntryJSON, _ := json.Marshal(logEntry)

			member := redis.Z{
				Score:  float64(time.Now().UnixMilli()),
				Member: logEntryJSON,
			}

			if err := w.db.ZAdd(ctx, sortedSetKey, member).Err(); err != nil {
				log.Printf(
					"ERRO CRÍTICO: Falha ao salvar log do pagamento %s. Movendo para DLQ.",
					req.CorrelationID,
				)
				w.db.LPush(ctx, dlqKey, payload)
				w.db.XAck(ctx, streamName, groupName, message.ID)
				continue
			}

			w.db.XAck(ctx, streamName, groupName, message.ID)
		}
	}
	return nil
}

func (w *Worker) processPayment(ctx context.Context, req PaymentRequest) (string, error) {
	const decisionThreshold = 150 // ms

	processorReq := PaymentProcessorRequest{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		RequestedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Busca o status de ambos os processadores com MGet
	healthKeys := []string{
		"health:default:failing",
		"health:default:minResponseTime",
		"health:fallback:failing",
	}
	results, err := w.db.MGet(ctx, healthKeys...).Result()
	if err != nil {
		log.Printf("AVISO: Falha ao buscar health status no Redis: %v. Tentando Default.", err)
		if err := w.callPaymentProcessor(ctx, w.processorDefaultURL, processorReq); err == nil {
			return "default", nil
		}
		return "", fmt.Errorf("falha ao buscar status e ao tentar processador default")
	}

	isDefaultFailing := results[0] != nil && results[0].(string) == "true"

	var defaultResponseTime int
	if results[1] != nil {
		defaultResponseTime, _ = strconv.Atoi(results[1].(string))
	}

	isFallbackFailing := results[2] != nil && results[2].(string) == "true"

	// Lógica de decisão
	useDefault := true
	if isDefaultFailing {
		useDefault = false
	} else if defaultResponseTime > decisionThreshold {
		useDefault = false
	}

	if useDefault {
		if err := w.callPaymentProcessor(ctx, w.processorDefaultURL, processorReq); err == nil {
			return "default", nil
		}
		if err := w.callPaymentProcessor(ctx, w.processorFallbackURL, processorReq); err == nil {
			return "fallback", nil
		}
	} else {
		if !isFallbackFailing {
			if err := w.callPaymentProcessor(ctx, w.processorFallbackURL, processorReq); err == nil {
				return "fallback", nil
			}
		}
		if err := w.callPaymentProcessor(ctx, w.processorDefaultURL, processorReq); err == nil {
			return "default", nil
		}
	}

	log.Printf("ERRO: Ambos os processadores falharam para o pagamento %s.", req.CorrelationID)
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

func DLQReanimator(ctx context.Context, db *redis.Client) {
	log.Println("Reanimador da DLQ iniciando...")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Reanimador da DLQ desligando.")
			return
		case <-ticker.C:
			payload, err := db.RPop(ctx, dlqKey).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				log.Printf("Erro ao ler da DLQ: %v", err)
				continue
			}

			args := &redis.XAddArgs{
				Stream: streamName,
				Values: map[string]any{"payload": payload},
			}
			if err := db.XAdd(ctx, args).Err(); err != nil {
				log.Printf("Erro ao re-enfileirar mensagem da DLQ: %v", err)
				db.LPush(ctx, dlqKey, payload)
			} else {
				log.Printf("Mensagem da DLQ re-enfileirada com sucesso.")
			}
		}
	}
}
