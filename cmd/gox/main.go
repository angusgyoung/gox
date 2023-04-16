package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/angusgyoung/gox/internal"
	"github.com/angusgyoung/gox/pkg"
)

func main() {
	log.Println("Starting gox...")
	ctx := context.Background()
	operator := internal.NewOperator(ctx)

	intervalStr := pkg.GetEnv("INTERVAL_MILLIS", "100")
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Panicf("Failed to parse interval '%s' to an integer", intervalStr)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		log.Println("Checking for events...")
		for {
			go func() {
				event, _ := operator.PublishPending(ctx)
				if event != nil {
					log.Printf("Published message with key '%s' to topic '%s' on partition '%d'\n",
						event.Key,
						event.Topic,
						event.Partition)
				}
			}()

			time.Sleep(time.Millisecond * time.Duration(interval))
		}
	}()

	<-sigChan
	log.Println("Stopping gox...")
	operator.Close()
}
