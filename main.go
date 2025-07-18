package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/diegodario88/carijo/cmd/api"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "storage:6379",
		Password: "",
		DB:       0,
	})

	var wg sync.WaitGroup
	httpServer := api.NewHttpServer(rdb)
	// worker := worker.New() // Futuro worker sera inicializado aqui.

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpServer.Run(); err != nil && err != http.ErrServerClosed {
			log.Printf("Servidor Http foi finalizado com erro: %v", err)
		}
	}()

	// wg.Add(1)
	// go func() {
	// 	  defer wg.Done()
	//    log.Println("Worker iniciando...")
	// 	  // Supondo que o worker tenha um método Run que aceita um contexto
	// 	  // para saber quando parar.
	// 	  if err := worker.Run(ctx); err != nil {
	// 		  log.Printf("Worker foi finalizado com erro: %v", err)
	//    }
	// }()

	<-ctx.Done()

	log.Println("Sinal de desligamento recebido. Iniciando graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Erro no desligamento do Servidor Http: %v", err)
	} else {
		log.Println("Servidor Http desligado com sucesso.")
	}

	// if err := worker.Shutdown(shutdownCtx); err != nil {
	// 	  log.Printf("Erro no desligamento do Worker: %v", err)
	// } else {
	//    log.Println("Worker desligado com sucesso.")
	// }

	wg.Wait()

	log.Println("Desligando cliente Redis...")
	if err := rdb.Close(); err != nil {
		log.Printf("Erro ao fechar conexão com Redis: %v", err)
	}

	log.Println("Processo de desligamento foi completo com sucesso!")
}
