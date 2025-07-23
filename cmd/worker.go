package cmd

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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diegodario88/carijo/pkg"
	"github.com/diegodario88/carijo/pkg/common"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	Consumer             string
	db                   *redis.Client
	breakerStore         *pkg.CircuitBreakerStore
	summaryStore         *pkg.InMemorySummaryStore
	processorDefaultURL  string
	processorFallbackURL string
	httpClient           *http.Client
}

func NewPaymentWorker(
	db *redis.Client,
	breaker *pkg.CircuitBreakerStore,
	summaryStore *pkg.InMemorySummaryStore,
) *Worker {
	return &Worker{
		Consumer:     common.Whoami,
		db:           db,
		breakerStore: breaker,
		summaryStore: summaryStore,
		processorDefaultURL: common.GetEnv(
			"PROCESSOR_DEFAULT_URL",
			"http://payment-processor-default:8080/payments",
		),
		processorFallbackURL: common.GetEnv(
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
			consumerID := fmt.Sprintf(
				"%s-%s",
				common.Whoami,
				hex.EncodeToString(randomBytes),
			)

			streams, err := w.db.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    common.GroupName,
				Consumer: consumerID,
				Streams:  []string{common.StreamName, ">"},
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
			var req common.PaymentRequest
			if err := json.Unmarshal([]byte(payload), &req); err != nil {
				log.Printf(
					"[Worker %d] Erro ao decodificar payload, descartando: %v",
					workerID,
					err,
				)
				w.db.XAck(ctx, common.StreamName, common.GroupName, message.ID)
				continue
			}

			if err := w.processPayment(ctx, req); err != nil {
				continue
			}

			w.db.XAck(ctx, common.StreamName, common.GroupName, message.ID)
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.createConsumerGroup(ctx)
	concurrency, _ := strconv.Atoi(common.GetEnv("WORKER_CONCURRENCY", "2"))

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
		Stream:   common.StreamName,
		Group:    common.GroupName,
		Count:    common.ClaimUnprocessedCount,
		MinIdle:  common.MinIdleDuration,
		Consumer: w.Consumer,
		Start:    "0-0",
	}

	claimedMessages, _, err := w.db.XAutoClaim(ctx, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("[Janitor] Erro ao tentar reclamar mensagens: %v", err)
		return
	}

	if len(claimedMessages) > 0 {
		var wg sync.WaitGroup

		for i, msg := range claimedMessages {
			wg.Add(1)
			go func(workerID int, message redis.XMessage) {
				defer wg.Done()
				w.processClaimedMessage(ctx, message, workerID)
			}(i, msg)
		}

		wg.Wait()
	}
}

func (w *Worker) processClaimedMessage(ctx context.Context, msg redis.XMessage, workerID int) {
	payload := msg.Values["payload"].(string)
	var req common.PaymentRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		log.Printf(
			"[Janitor Worker %d] Erro ao decodificar payload de mensagem reclamada, descartando: %v",
			workerID,
			err,
		)
		w.db.XAck(ctx, common.StreamName, common.GroupName, msg.ID)
		return
	}

	if err := w.processPayment(ctx, req); err != nil {
		return
	}

	w.db.XAck(ctx, common.StreamName, common.GroupName, msg.ID)
}

func (w *Worker) createConsumerGroup(ctx context.Context) {
	err := w.db.XGroupCreateMkStream(ctx, common.StreamName, common.GroupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Erro inesperado ao criar consumer group: %v", err)
	}
}

func (w *Worker) processPayment(ctx context.Context, req common.PaymentRequest) error {
	if w.breakerStore.Allow("default") {
		err := w.executeAndMonitor(ctx, "default", w.processorDefaultURL, req)
		if err == nil {
			return nil
		}
	}

	if w.breakerStore.Allow("fallback") {
		err := w.executeAndMonitor(ctx, "fallback", w.processorFallbackURL, req)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf(
		"ambos processadores ('default' e 'fallback') estão indisponíveis ou com circuitos abertos",
	)
}

func (w *Worker) executeAndMonitor(
	ctx context.Context,
	processorName, url string,
	req common.PaymentRequest,
) error {
	requestedAtTime := time.Now().UTC()
	requestedAtStr := requestedAtTime.Format(time.RFC3339Nano)

	body := common.PaymentProcessorRequest{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		RequestedAt:   requestedAtStr,
	}
	jsonBody, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("falha ao criar http request para %s: %w", processorName, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
		w.breakerStore.RecordFailure(processorName)
		return fmt.Errorf("falha na chamada http para %s: %w", processorName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		w.breakerStore.RecordFailure(processorName)
		return fmt.Errorf("processador %s retornou status não-2xx: %s", processorName, resp.Status)
	}

	w.breakerStore.RecordSuccess(processorName)
	w.summaryStore.Save(processorName, requestedAtTime)

	return nil
}
