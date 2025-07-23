package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/diegodario88/carijo/pkg"
	"github.com/diegodario88/carijo/pkg/common"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const paymentAmount = "19.90"

type SummaryResponse struct {
	Default struct {
		TotalRequests int64   `json:"totalRequests"`
		TotalAmount   float64 `json:"totalAmount"`
	} `json:"default"`
	Fallback struct {
		TotalRequests int64   `json:"totalRequests"`
		TotalAmount   float64 `json:"totalAmount"`
	} `json:"fallback"`
}

type InternalSummaryResponse struct {
	DefaultCount  int64 `json:"default_count"`
	FallbackCount int64 `json:"fallback_count"`
}

type HttpServer struct {
	port         string
	db           *redis.Client
	summaryStore *pkg.InMemorySummaryStore
	app          *fiber.App
	httpClient   *http.Client
}

func NewHttpServer(db *redis.Client, summaryStore *pkg.InMemorySummaryStore) *HttpServer {
	app := fiber.New(fiber.Config{
		IdleTimeout: 5 * time.Second,
	})

	s := &HttpServer{
		port:         ":3000",
		db:           db,
		summaryStore: summaryStore,
		app:          app,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
	}

	s.registerRoutes()

	return s
}

func (api *HttpServer) Run() error {
	log.Println("Inicializando servidor HTTP na porta " + api.port)
	return api.app.Listen(api.port)
}

func (api *HttpServer) Shutdown(ctx context.Context) error {
	log.Println("Desligando servidor HTTP...")
	return api.app.ShutdownWithContext(ctx)
}

func (s *HttpServer) registerRoutes() {
	s.app.Post("/payments", s.handlePayments)
	s.app.Get("/payments-summary", s.handleGetSummary)
	s.app.Get("/internal/summary", s.handleGetInternalSummary)
	s.app.Post("/purge-payments", s.handlePurgeAllData)
	s.app.Post("/internal/purge", s.handleInternalPurge)
}

func (s *HttpServer) handlePayments(c fiber.Ctx) error {
	var req common.PaymentRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "payload invalido"})
	}
	payload, _ := json.Marshal(req)
	args := &redis.XAddArgs{
		Stream: common.StreamName,
		Values: map[string]any{"payload": payload},
	}
	if err := s.db.XAdd(c.Context(), args).Err(); err != nil {
		log.Printf("Erro ao adicionar na stream: %v", err)
		return c.Status(fiber.StatusInternalServerError).
			JSON(fiber.Map{"error": "falha ao enfileirar"})
	}

	return c.Status(fiber.StatusAccepted).SendString("")
}

func (s *HttpServer) handleGetInternalSummary(c fiber.Ctx) error {
	from, to, _ := parseTimeRange(c)
	localSummary := s.summaryStore.GetSummary(from, to)

	return c.Status(fiber.StatusOK).JSON(InternalSummaryResponse{
		DefaultCount:  localSummary.DefaultCount,
		FallbackCount: localSummary.FallbackCount,
	})
}

func (s *HttpServer) handleGetSummary(c fiber.Ctx) error {
	from, to, useFilter := parseTimeRange(c)

	localSummary := s.summaryStore.GetSummary(from, to)

	totalDefault := localSummary.DefaultCount
	totalFallback := localSummary.FallbackCount

	remoteSummary, err := s.fetchRemoteSummary(c.Context(), useFilter, from, to)
	if err != nil {
		log.Printf("Falha ao buscar resumo da outra instancia: %v", err)
	} else {
		totalDefault += remoteSummary.DefaultCount
		totalFallback += remoteSummary.FallbackCount
	}

	amountDecimal, _ := decimal.NewFromString(paymentAmount)
	defaultAmount, _ := decimal.NewFromInt(totalDefault).Mul(amountDecimal).Float64()
	fallbackAmount, _ := decimal.NewFromInt(totalFallback).Mul(amountDecimal).Float64()

	resp := SummaryResponse{}
	resp.Default.TotalRequests = totalDefault
	resp.Default.TotalAmount = defaultAmount
	resp.Fallback.TotalRequests = totalFallback
	resp.Fallback.TotalAmount = fallbackAmount

	return c.Status(fiber.StatusOK).JSON(resp)
}

func (s *HttpServer) handleInternalPurge(c fiber.Ctx) error {
	log.Println("Recebida requisição de purge interno. Limpando store em memória...")
	s.summaryStore.Purge()
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "in-memory store purgada"})
}

func (s *HttpServer) handlePurgeAllData(c fiber.Ctx) error {
	s.summaryStore.Purge()

	if err := s.triggerRemotePurge(c.Context()); err != nil {
		log.Printf("Falha ao triggar purge na outra instancia (%s): %v", common.Twin, err)
		return c.Status(fiber.StatusInternalServerError).
			JSON(fiber.Map{"error": "falha ao purgar a outra instancia"})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "todos os dados foram purgados"})
}

func (s *HttpServer) triggerRemotePurge(ctx context.Context) error {
	url := fmt.Sprintf("http://%s:3000/internal/purge", common.Twin)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("falha ao criar request de purge para outra instancia: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("falha na chamada http de purge para outra instancia: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("outra instancia retornou status não-OK no purge: %s", resp.Status)
	}

	log.Println("Purge na outra instancia executado com sucesso.")
	return nil
}

func (s *HttpServer) fetchRemoteSummary(
	ctx context.Context,
	useFilter bool,
	from, to time.Time,
) (*InternalSummaryResponse, error) {
	url := fmt.Sprintf("http://%s:3000/internal/summary", common.Twin)
	if useFilter {
		url = fmt.Sprintf(
			"%s?from=%s&to=%s",
			url,
			from.Format(time.RFC3339Nano),
			to.Format(time.RFC3339Nano),
		)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar request para outra instancia: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha na chamada http para outra instancia: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("outra instancia retornou status não-OK: %s", resp.Status)
	}

	var remoteSummary InternalSummaryResponse
	if err := json.NewDecoder(resp.Body).Decode(&remoteSummary); err != nil {
		return nil, fmt.Errorf("falha ao decodificar resposta da outra instancia: %w", err)
	}

	return &remoteSummary, nil
}

func parseTimeRange(c fiber.Ctx) (time.Time, time.Time, bool) {
	fromStr := c.Query("from")
	toStr := c.Query("to")
	useFilter := fromStr != "" && toStr != ""

	if !useFilter {
		return time.Time{}, time.Time{}, false
	}

	from, err := time.Parse(time.RFC3339Nano, fromStr)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	to, err := time.Parse(time.RFC3339Nano, toStr)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}

	return from, to, true
}
