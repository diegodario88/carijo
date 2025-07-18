package api

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
)

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
		return c.SendString("/payments")
	})
	app.Get("/payments-summary", func(c fiber.Ctx) error {
		return c.SendString("/payments-summary")
	})
	app.Post("/purge-payments", func(c fiber.Ctx) error {
		return c.SendString("/purge-payments")
	})

	return &HttpServer{
		port: ":3000",
		db:   db,
		app:  app,
	}
}

func (api *HttpServer) Run() error {
	log.Println("Inicializando servidor HTTP na porta " + api.port)
	return api.app.Listen(api.port)
}

func (api *HttpServer) Shutdown(ctx context.Context) error {
	log.Println("Desligando servidor HTTP...")
	return api.app.ShutdownWithContext(ctx)
}
