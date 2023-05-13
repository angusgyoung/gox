package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/angusgyoung/gox/internal"
)

func main() {
	log.Println("Starting gox...")
	ctx := context.Background()

	operator, err := internal.NewOperator(ctx, &internal.OperatorConfig{
		PollInterval: internal.GetEnvInt("GOX_POLL_INTERVAL", 100),
		BatchSize:    internal.GetEnvInt("GOX_BATCH_SIZE", 50),
		DatabaseUrl:  internal.GetReqEnvString("GOX_DB_URL"),
		BrokerUrls:   internal.GetReqEnvString("GOX_BROKER_URLS"),
		Topics:       internal.GetReqEnvStringList("GOX_TOPICS"),
	})
	if err != nil {
		log.Fatalf("Failed to create operator: %s\n", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		log.Println("Polling for events...")
		for {
			err := operator.Execute(ctx)
			if err != nil {
				log.Fatalf("Operator error: %s\n", err)
			}
		}
	}()

	<-sigChan
	log.Println("Stopping gox...")
	operator.Close(ctx)
}
