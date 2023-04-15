package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/angusgyoung/gox/internal"
)

func main() {
	log.Println("Starting gox")
	ctx := context.Background()
	operator := internal.NewOperator(ctx)

	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			event, _ := operator.PublishPending(ctx)
			if event != nil {
				log.Printf("Published message with key '%s' to topic '%s'\n", event.Key, event.Topic)
			}

			time.Sleep(time.Second * 1)
		}
	}()
	sig := <-cancelChan
	// TODO gracefully shutdown on termination
	log.Printf("Caught signal %v\n", sig)
}
