package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/diegodario88/carijo/cmd"
	"github.com/diegodario88/carijo/pkg"
	"github.com/diegodario88/carijo/pkg/common"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	breakerStore := pkg.NewCircuitBreakerStore()
	summaryStore := pkg.NewInMemorySummaryStore()

	concurrency, _ := strconv.Atoi(common.GetEnv("WORKER_CONCURRENCY", "7"))
	log.Printf("Iniciando com %v trabalhadores", concurrency)
	queue := pkg.NewQueue(20000, concurrency, breakerStore, summaryStore)

	var wg sync.WaitGroup
	httpServer := cmd.NewHttpServer(queue, summaryStore)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpServer.Run(); err != nil && err != http.ErrServerClosed {
			log.Printf("Servidor Http foi finalizado com erro: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Run(ctx)
	}()

	<-ctx.Done()

	log.Println("Sinal de desligamento recebido. Iniciando graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Erro no desligamento do Servidor Http: %v", err)
	} else {
		log.Println("Servidor Http desligado com sucesso.")
	}

	log.Println("Aguardando todos os serviços finalizarem...")
	wg.Wait()

	log.Println("Processo de desligamento foi completo com sucesso!")
}
