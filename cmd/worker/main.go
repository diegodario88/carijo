package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diegodario88/carijo/pkg/utils"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

var hostname string = "unknown"

const streamName = "payments_stream"
const groupName = "payment_processors"
const paymentsHashKey = "payments"
const fallbackThreshold = 0.8

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentProcessorRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	RequestedAt   string          `json:"requestedAt"`
}

type ProcessedPayment struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	Processor     string          `json:"processor"`
	Status        string          `json:"status"`
	CreatedAt     string          `json:"createdAt"`
}

type HealthInfo struct {
	DefaultFailing          bool
	DefaultMinResponseTime  int
	FallbackFailing         bool
	FallbackMinResponseTime int
}

type Worker struct {
	Consumer             string
	db                   *redis.Client
	processorDefaultURL  string
	processorFallbackURL string
	httpClient           *http.Client
}

func NewPaymentWorker(db *redis.Client) *Worker {
	hostname, _ = os.Hostname()

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
				MaxIdleConns:        100,
				DisableCompression:  true,
				MaxIdleConnsPerHost: 50,
				IdleConnTimeout:     120 * time.Second,
				MaxConnsPerHost:     50,
				DialContext: (&net.Dialer{
					Timeout:   2 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		},
	}
}

func (w *Worker) paymentProcessorWorker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker goroutine %d for consumer %s desligando", workerID, w.Consumer)
			return
		default:
			randomBytes := make([]byte, 8)
			rand.Read(randomBytes)
			consumerID := fmt.Sprintf("%s-%s", hostname, hex.EncodeToString(randomBytes))

			streams, err := w.db.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerID,
				Streams:  []string{streamName, ">"},
				Count:    1,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if !errors.Is(err, redis.Nil) && !errors.Is(err, context.Canceled) {
					log.Printf("[Worker %d] Erro ao ler da stream: %v", workerID, err)
				}
				continue
			}

			if len(streams) == 0 || len(streams[0].Messages) == 0 {
				continue
			}

			message := streams[0].Messages[0]
			payload := message.Values["payload"].(string)
			var req PaymentRequest
			if err := json.Unmarshal([]byte(payload), &req); err != nil {
				log.Printf(
					"[Worker %d] Erro ao decodificar payload, descartando: %v",
					workerID,
					err,
				)
				w.db.XAck(ctx, streamName, groupName, message.ID)
				continue
			}

			if err := w.processPayment(ctx, req); err != nil {
				continue
			}

			w.db.XAck(ctx, streamName, groupName, message.ID)
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.createConsumerGroup(ctx)
	concurrency, _ := strconv.Atoi(utils.GetEnv("WORKER_CONCURRENCY", "2"))

	var wg sync.WaitGroup

	for i := 1; i <= concurrency; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			w.paymentProcessorWorker(ctx, workerNum)
		}(i)
	}

	<-ctx.Done()
	log.Println("Sinal de desligamento recebido, aguardando workers finalizarem...")
	wg.Wait()
	log.Println("Todos os workers finalizaram.")
	return nil
}

func (w *Worker) RunJanitor(ctx context.Context) {
	w.createConsumerGroup(ctx)
	w.Consumer = fmt.Sprintf("%s-janitor", w.Consumer)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("Iniciando worker em modo Janitor para reclamar mensagens pendentes...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Janitor desligando.")
			return
		case <-ticker.C:
			w.claimAndReprocess(ctx)
		}
	}
}

func (w *Worker) claimAndReprocess(ctx context.Context) {
	args := &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: w.Consumer,
		MinIdle:  2 * time.Second,
		Start:    "0-0",
		Count:    10,
	}

	claimedMessages, _, err := w.db.XAutoClaim(ctx, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("[Janitor] Erro ao tentar reclamar mensagens: %v", err)
		return
	}

	if len(claimedMessages) > 0 {
		log.Printf(
			"[Janitor] Reclamou %d mensagens pendentes para reprocessamento.",
			len(claimedMessages),
		)

		var wg sync.WaitGroup
		numWorkers := len(claimedMessages)

		for i, msg := range claimedMessages {
			wg.Add(1)
			go func(workerID int, message redis.XMessage) {
				defer wg.Done()
				w.processClaimedMessage(ctx, message, workerID)
			}(i, msg)
		}

		wg.Wait()
		log.Printf("[Janitor] Finalizou processamento de %d mensagens.", numWorkers)
	}
}

func (w *Worker) processClaimedMessage(ctx context.Context, msg redis.XMessage, workerID int) {
	payload := msg.Values["payload"].(string)
	var req PaymentRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		log.Printf(
			"[Janitor Worker %d] Erro ao decodificar payload de mensagem reclamada, descartando: %v",
			workerID,
			err,
		)
		w.db.XAck(ctx, streamName, groupName, msg.ID)
		return
	}

	if err := w.processPayment(ctx, req); err != nil {
		return
	}

	w.db.XAck(ctx, streamName, groupName, msg.ID)
}

func (w *Worker) createConsumerGroup(ctx context.Context) {
	err := w.db.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Erro inesperado ao criar consumer group: %v", err)
	}
}

func (w *Worker) processPayment(ctx context.Context, req PaymentRequest) error {
	health, err := w.retrieveHealthStates(ctx)
	if err != nil {
		return fmt.Errorf("falha ao buscar status de saude: %w", err)
	}

	processorURL := w.processorDefaultURL
	processorType := "DEFAULT"
	status := "PROCESSED_DEFAULT"
	useFallback := health.DefaultFailing

	if !useFallback && health.DefaultMinResponseTime > 0 && health.FallbackMinResponseTime > 0 {
		defaultTime := decimal.NewFromInt(int64(health.DefaultMinResponseTime))
		fallbackTime := decimal.NewFromInt(int64(health.FallbackMinResponseTime))
		threshold := decimal.NewFromFloat(fallbackThreshold)

		advantage := defaultTime.Sub(fallbackTime).Div(defaultTime)

		if advantage.GreaterThan(threshold) {
			useFallback = true
		}
	}

	if useFallback {
		processorURL = w.processorFallbackURL
		processorType = "FALLBACK"
		status = "PROCESSED_FALLBACK"
	}

	requestedAt := time.Now().UTC().Format(time.RFC3339Nano)
	body := PaymentProcessorRequest{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		RequestedAt:   requestedAt,
	}
	jsonBody, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", processorURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("falha ao criar http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("falha na chamada http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		w.db.Set(ctx, "health:"+strings.ToLower(processorType)+":failing", "true", 10*time.Second)
		return fmt.Errorf("processador retornou status não-2xx: %s", resp.Status)
	}

	processedPayment := ProcessedPayment{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		Status:        status,
		Processor:     processorType,
		CreatedAt:     requestedAt,
	}
	paymentData, err := json.Marshal(processedPayment)
	if err != nil {
		return fmt.Errorf("falha ao serializar dados do pagamento: %w", err)
	}

	if err := w.db.HSet(ctx, paymentsHashKey, req.CorrelationID, paymentData).Err(); err != nil {
		return fmt.Errorf("falha ao salvar pagamento no redis: %w", err)
	}

	return nil
}

func (w *Worker) retrieveHealthStates(ctx context.Context) (*HealthInfo, error) {
	keys := []string{
		"health:default:failing",
		"health:default:minResponseTime",
		"health:fallback:failing",
		"health:fallback:minResponseTime",
	}
	results, err := w.db.MGet(ctx, keys...).Result()
	if err != nil {
		return &HealthInfo{DefaultFailing: false, FallbackFailing: false}, nil
	}

	health := &HealthInfo{}

	if len(results) > 0 && results[0] != nil {
		health.DefaultFailing, _ = strconv.ParseBool(results[0].(string))
	}
	if len(results) > 1 && results[1] != nil {
		val, _ := strconv.Atoi(results[1].(string))
		health.DefaultMinResponseTime = val
	}
	if len(results) > 2 && results[2] != nil {
		health.FallbackFailing, _ = strconv.ParseBool(results[2].(string))
	}
	if len(results) > 3 && results[3] != nil {
		val, _ := strconv.Atoi(results[3].(string))
		health.FallbackMinResponseTime = val
	}

	return health, nil
}
