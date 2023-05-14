package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/angusgyoung/gox/internal"
)

func main() {
	logLevelStr := internal.GetEnvString("GOX_LOG_LEVEL", "INFO")
	logLevel, err := log.ParseLevel(logLevelStr)
	if err != nil {
		log.Warnf("Failed to parse parameter '%s' to a log level, defaulting to warn", logLevelStr)
		logLevel = log.WarnLevel
	}
	log.SetLevel(logLevel)
	log.SetFormatter(&log.JSONFormatter{})
	log.Info("Starting gox...")

	ctx := context.Background()
	operator, err := internal.NewOperator(ctx, &internal.OperatorConfig{
		PollInterval: internal.GetEnvInt("GOX_POLL_INTERVAL", 100),
		BatchSize:    internal.GetEnvInt("GOX_BATCH_SIZE", 50),
		DatabaseUrl:  internal.GetReqEnvString("GOX_DB_URL"),
		BrokerUrls:   internal.GetReqEnvString("GOX_BROKER_URLS"),
		Topics:       internal.GetReqEnvStringList("GOX_TOPICS"),
	})
	if err != nil {
		log.WithError(err).Fatal("Failed to create operator")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		log.Info("Polling for events...")
		for {
			err := operator.Execute(ctx)
			if err != nil {
				log.WithError(err).Fatal("Operator failed")
			}
		}
	}()

	<-sigChan
	log.Info("Stopping gox...")
	operator.Close(ctx)
}
