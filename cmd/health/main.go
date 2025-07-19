package health

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/diegodario88/carijo/pkg/utils"
	"github.com/redis/go-redis/v9"
)

const healthCheckInterval = 5 * time.Second
const statusTTL = 10 * time.Second

type HealthStatus struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type HealthChecker struct {
	db                   *redis.Client
	httpClient           *http.Client
	processorDefaultURL  string
	processorFallbackURL string
}

func NewHealthChecker(db *redis.Client) *HealthChecker {
	return &HealthChecker{
		db: db,
		httpClient: &http.Client{
			Timeout: 4 * time.Second,
		},
		processorDefaultURL: utils.GetEnv(
			"PROCESSOR_DEFAULT_URL_HEALTH",
			"http://payment-processor-default:8080/payments/service-health",
		),
		processorFallbackURL: utils.GetEnv(
			"PROCESSOR_FALLBACK_URL_HEALTH",
			"http://payment-processor-fallback:8080/payments/service-health",
		),
	}
}

func (hc *HealthChecker) Run(ctx context.Context) {
	log.Println("Health Checker iniciando...")
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	hc.checkAll(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Health Checker desligando.")
			return
		case <-ticker.C:
			hc.checkAll(ctx)
		}
	}
}

func (hc *HealthChecker) checkAll(ctx context.Context) {
	go hc.checkProcessor(ctx, "default", hc.processorDefaultURL)
	go hc.checkProcessor(ctx, "fallback", hc.processorFallbackURL)
}

func (hc *HealthChecker) checkProcessor(ctx context.Context, name, url string) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("HEALTH: Erro ao criar requisição para %s: %v", name, err)
		return
	}

	resp, err := hc.httpClient.Do(req)
	if err != nil {
		log.Printf("HEALTH: Falha na chamada para %s: %v. Marcando como 'failing'.", name, err)
		hc.db.Set(ctx, "health:"+name+":failing", "true", statusTTL)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf(
			"HEALTH: Status não-OK para %s: %d. Marcando como 'failing'.",
			name,
			resp.StatusCode,
		)
		hc.db.Set(ctx, "health:"+name+":failing", "true", statusTTL)
		return
	}

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		log.Printf("HEALTH: Erro ao decodificar resposta de %s: %v", name, err)
		return
	}

	pipe := hc.db.Pipeline()
	pipe.Set(ctx, "health:"+name+":failing", status.Failing, statusTTL)
	pipe.Set(ctx, "health:"+name+":minResponseTime", status.MinResponseTime, statusTTL)
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("HEALTH: Erro ao salvar status de %s no Redis: %v", name, err)
	}
}
