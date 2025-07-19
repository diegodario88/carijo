package api

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const sortedSetKey = "payments:log"
const streamName = "payments_stream"

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentLogEntry struct {
	CorrelationID string          `json:"correlationId"`
	Processor     string          `json:"processor"`
	Amount        decimal.Decimal `json:"amount"`
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
			Values: map[string]interface{}{"payload": payload},
		}
		if err := db.XAdd(c.Context(), args).Err(); err != nil {
			log.Printf("Erro ao adicionar na stream: %v", err)
			return c.Status(fiber.StatusInternalServerError).
				JSON(fiber.Map{"error": "falha ao enfileirar"})
		}
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"message": "pagamento recebido"})
	})

	app.Get("/payments-summary", func(c fiber.Ctx) error {
		from, to, err := parseTimeRange(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}

		logEntries, err := db.ZRangeByScore(c.Context(), sortedSetKey, &redis.ZRangeBy{
			Min: from,
			Max: to,
		}).Result()

		if err != nil {
			log.Printf("Erro ao buscar dados do summary no Redis: %v", err)
			return c.Status(fiber.StatusInternalServerError).
				JSON(fiber.Map{"error": "falha ao buscar resumo"})
		}

		var resp SummaryResponse
		totalAmountDefault := decimal.Zero
		totalAmountFallback := decimal.Zero

		for _, entryJSON := range logEntries {
			var entry PaymentLogEntry
			if err := json.Unmarshal([]byte(entryJSON), &entry); err != nil {
				log.Printf("AVISO: Falha ao decodificar entrada do log: %v", err)
				continue
			}

			switch entry.Processor {
			case "default":
				resp.Default.TotalRequests++
				totalAmountDefault = totalAmountDefault.Add(entry.Amount)
			case "fallback":
				resp.Fallback.TotalRequests++
				totalAmountFallback = totalAmountFallback.Add(entry.Amount)
			}
		}

		resp.Default.TotalAmount, _ = totalAmountDefault.Float64()
		resp.Fallback.TotalAmount, _ = totalAmountFallback.Float64()

		return c.Status(fiber.StatusOK).JSON(resp)
	})

	app.Post("/purge-payments", func(c fiber.Ctx) error {
		pipe := db.Pipeline()
		pipe.Del(c.Context(), sortedSetKey)
		pipe.XTrimMaxLen(c.Context(), streamName, 0)

		if _, err := pipe.Exec(c.Context()); err != nil {
			log.Printf("Erro ao purgar dados do Redis: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "falha ao limpar os dados",
			})
		}

		log.Println("Dados de pagamento purgados com sucesso.")
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"message": "todos os dados de pagamento foram purgados",
		})
	})

	return &HttpServer{
		port: ":3000",
		db:   db,
		app:  app,
	}
}

func parseTimeRange(c fiber.Ctx) (string, string, error) {
	fromStr := c.Query("from")
	toStr := c.Query("to")

	if fromStr == "" {
		fromStr = "-inf"
	} else {
		t, err := time.Parse(time.RFC3339Nano, fromStr)
		if err != nil {
			return "", "", fiber.NewError(fiber.StatusBadRequest, "formato de timestamp 'from' invalido")
		}
		fromStr = strconv.FormatInt(t.UnixMilli(), 10)
	}

	if toStr == "" {
		toStr = "+inf"
	} else {
		t, err := time.Parse(time.RFC3339Nano, toStr)
		if err != nil {
			return "", "", fiber.NewError(fiber.StatusBadRequest, "formato de timestamp 'to' invalido")
		}
		toStr = strconv.FormatInt(t.UnixMilli(), 10)
	}

	return fromStr, toStr, nil
}

func (api *HttpServer) Run() error {
	log.Println("Inicializando servidor HTTP na porta " + api.port)
	return api.app.Listen(api.port)
}

func (api *HttpServer) Shutdown(ctx context.Context) error {
	log.Println("Desligando servidor HTTP...")
	return api.app.ShutdownWithContext(ctx)
}

