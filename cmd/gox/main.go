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
	operator := internal.NewOperator(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		log.Println("Checking for events...")
		for {
			err := operator.PublishPending(ctx)
			if err != nil {
				log.Fatalf("Operator error: %s\n", err)
			}
		}
	}()

	<-sigChan
	log.Println("Stopping gox...")
	operator.Close()
}
