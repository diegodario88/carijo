package pkg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/diegodario88/carijo/pkg/common"
)

type Job struct {
	Payment common.PaymentRequest
	Retries int
}

type Queue struct {
	jobs         chan Job
	breakerStore *CircuitBreakerStore
	summaryStore *InMemorySummaryStore
	httpClient   *http.Client
	concurrency  int

	processorDefaultURL  string
	processorFallbackURL string
}

func NewQueue(
	bufferSize int,
	concurrency int,
	breakerStore *CircuitBreakerStore,
	summaryStore *InMemorySummaryStore,
) *Queue {
	return &Queue{
		jobs:         make(chan Job, bufferSize),
		breakerStore: breakerStore,
		summaryStore: summaryStore,
		concurrency:  concurrency,
		processorDefaultURL: common.GetEnv(
			"PROCESSOR_DEFAULT_URL",
			"http://payment-processor-default:8080/payments",
		),
		processorFallbackURL: common.GetEnv(
			"PROCESSOR_FALLBACK_URL",
			"http://payment-processor-fallback:8080/payments",
		),
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        1000,
				DisableCompression:  true,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     120 * time.Second,
				MaxConnsPerHost:     10000,
				DialContext: (&net.Dialer{
					Timeout:   2 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		},
	}
}

func (q *Queue) Dispatch(payment common.PaymentRequest) {
	job := Job{Payment: payment}
	q.jobs <- job
}

func (q *Queue) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := range q.concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			q.worker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	log.Println("Todos os workers da fila foram finalizados.")
}

func (q *Queue) worker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-q.jobs:
			if err := q.processPayment(ctx, job.Payment); err != nil {
				q.retry(job)
			}
		}
	}
}

func (q *Queue) retry(job Job) {
	if job.Retries < common.MaxRetries {
		job.Retries++

		delay := common.RetryDelay * time.Duration(1<<(job.Retries-1))
		time.AfterFunc(delay, func() {
			q.jobs <- job
		})
	} else {
		log.Printf(
			"Pagamento %s descartado após %d tentativas.",
			job.Payment.CorrelationID,
			common.MaxRetries,
		)
	}
}

func (q *Queue) processPayment(ctx context.Context, req common.PaymentRequest) error {
	if q.breakerStore.Allow("default") {
		err := q.executeAndMonitor(ctx, "default", q.processorDefaultURL, req)
		if err == nil {
			return nil
		}
	}

	if q.breakerStore.Allow("fallback") {
		err := q.executeAndMonitor(ctx, "fallback", q.processorFallbackURL, req)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf(
		"ambos processadores ('default' e 'fallback') estão indisponíveis ou com circuitos abertos para o CorrelationID %s",
		req.CorrelationID,
	)
}

func (q *Queue) executeAndMonitor(
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

	resp, err := q.httpClient.Do(httpReq)
	if err != nil {
		q.breakerStore.RecordFailure(processorName)
		return fmt.Errorf("falha na chamada http para %s: %w", processorName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		q.breakerStore.RecordFailure(processorName)
		return fmt.Errorf("processador %s retornou status não-2xx: %s", processorName, resp.Status)
	}

	q.breakerStore.RecordSuccess(processorName)
	q.summaryStore.Save(processorName, requestedAtTime)

	return nil
}
