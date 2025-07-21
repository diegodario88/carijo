package api

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const streamName = "payments_stream"
const paymentsHashKey = "payments"

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

type summaryAggregator struct {
	Default struct {
		TotalRequests int64
		TotalAmount   decimal.Decimal
	}
	Fallback struct {
		TotalRequests int64
		TotalAmount   decimal.Decimal
	}
}

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

type HttpServer struct {
	port string
	db   *redis.Client
	app  *fiber.App
}

func NewHttpServer(db *redis.Client) *HttpServer {
	app := fiber.New(fiber.Config{
		IdleTimeout: 5 * time.Second,
	})

	app.Post("/payments", func(c fiber.Ctx) error {
		var req PaymentRequest
		if err := c.Bind().Body(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "payload invalido"})
		}
		payload, _ := json.Marshal(req)
		args := &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]any{"payload": payload},
		}
		if err := db.XAdd(c.Context(), args).Err(); err != nil {
			log.Printf("Erro ao adicionar na stream: %v", err)
			return c.Status(fiber.StatusInternalServerError).
				JSON(fiber.Map{"error": "falha ao enfileirar"})
		}
		return c.Status(fiber.StatusAccepted).SendString("")
	})

	app.Get("/payments-summary", func(c fiber.Ctx) error {
		fromStr := c.Query("from")
		toStr := c.Query("to")
		useFilter := fromStr != "" && toStr != ""

		var from, to time.Time
		var err error
		if useFilter {
			from, err = time.Parse(time.RFC3339Nano, fromStr)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).
					JSON(fiber.Map{"error": "formato de timestamp 'from' invalido"})
			}
			to, err = time.Parse(time.RFC3339Nano, toStr)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).
					JSON(fiber.Map{"error": "formato de timestamp 'to' invalido"})
			}
		}

		paymentsData, err := db.HGetAll(c.Context(), paymentsHashKey).Result()
		if err != nil {
			log.Printf("Erro ao buscar dados do summary no Redis: %v", err)
			return c.Status(fiber.StatusInternalServerError).
				JSON(fiber.Map{"error": "falha ao buscar resumo"})
		}

		aggregator := summarizePayments(paymentsData, from, to, useFilter)

		resp := SummaryResponse{}
		resp.Default.TotalRequests = aggregator.Default.TotalRequests
		resp.Default.TotalAmount, _ = aggregator.Default.TotalAmount.Float64()
		resp.Fallback.TotalRequests = aggregator.Fallback.TotalRequests
		resp.Fallback.TotalAmount, _ = aggregator.Fallback.TotalAmount.Float64()

		return c.Status(fiber.StatusOK).JSON(resp)
	})

	app.Post("/purge-all-data", func(c fiber.Ctx) error {
		pipe := db.Pipeline()
		pipe.Del(c.Context(), paymentsHashKey)
		pipe.Del(c.Context(), streamName)
		pipe.Del(c.Context(), "health:default:failing", "health:default:minResponseTime")
		pipe.Del(c.Context(), "health:fallback:failing", "health:fallback:minResponseTime")

		if _, err := pipe.Exec(c.Context()); err != nil {
			log.Printf("Erro ao purgar dados do Redis: %v", err)
			return c.Status(fiber.StatusInternalServerError).
				JSON(fiber.Map{"error": "falha ao limpar os dados"})
		}

		log.Println("Todos os dados foram purgados com sucesso.")
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "todos os dados foram purgados"})
	})

	return &HttpServer{
		port: ":3000",
		db:   db,
		app:  app,
	}
}

func summarizePayments(
	paymentsData map[string]string,
	from, to time.Time,
	useFilter bool,
) summaryAggregator {
	agg := summaryAggregator{}
	agg.Default.TotalAmount = decimal.Zero
	agg.Fallback.TotalAmount = decimal.Zero

	for _, paymentDataStr := range paymentsData {
		var payment ProcessedPayment
		if err := json.Unmarshal([]byte(paymentDataStr), &payment); err != nil {
			continue
		}

		if useFilter {
			createdAt, err := time.Parse(time.RFC3339Nano, payment.CreatedAt)
			if err != nil || createdAt.Before(from) || createdAt.After(to) {
				continue
			}
		}

		if payment.Processor == "DEFAULT" && payment.Status == "PROCESSED_DEFAULT" {
			agg.Default.TotalRequests++
			agg.Default.TotalAmount = agg.Default.TotalAmount.Add(payment.Amount)
		} else if payment.Processor == "FALLBACK" && payment.Status == "PROCESSED_FALLBACK" {
			agg.Fallback.TotalRequests++
			agg.Fallback.TotalAmount = agg.Fallback.TotalAmount.Add(payment.Amount)
		}
	}
	return agg
}

func (api *HttpServer) Run() error {
	log.Println("Inicializando servidor HTTP na porta " + api.port)
	return api.app.Listen(api.port)
}

func (api *HttpServer) Shutdown(ctx context.Context) error {
	log.Println("Desligando servidor HTTP...")
	return api.app.ShutdownWithContext(ctx)
}

