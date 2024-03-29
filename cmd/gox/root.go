package gox

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/angusgyoung/gox/internal/operator"
	"github.com/angusgyoung/gox/internal/telemetry"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type requiredParameter struct {
	name                string
	flag                string
	environmentVariable string
}

var (
	pollInterval       int
	batchSize          int
	dbUrl              string
	brokerUrls         string
	topics             []string
	logLevel           string
	logFormat          string
	enableTelemetry    bool
	completionMode     string
	requiredParameters = []requiredParameter{
		{
			"dbUrl",
			"db",
			"GOX_DB_URL",
		},
		{
			"brokers",
			"brokers",
			"GOX_BROKERS",
		},
		{
			"topics",
			"topics",
			"GOX_TOPICS",
		},
	}

	rootCmd = &cobra.Command{
		Use:   "gox",
		Short: "A small, scalable outbox publisher for Postgres/Kafka",
		Long: `
		A small outbox publisher for Postgres/Kafka, with support for kafka-backed 
		parallelisation and rebalancing. Intended to work nicely as a sidecar to a 
		container performing transactional publication.`,

		Run: func(cmd *cobra.Command, args []string) {
			logLevel := parseLogLevel(viper.GetString("logLevel"))
			log.SetLevel(logLevel)

			logFormat := parseLogFormat(viper.GetString("logFormat"))
			log.SetFormatter(logFormat)

			completionMode := parseCompletionMode(viper.GetString("completionMode"))

			log.Info("Starting gox...")

			validateConfig()

			ctx := context.Background()

			if viper.GetBool("enableTelemetry") {
				log.Warn("Telemetry support is experimental")
				err := telemetry.Initialise()
				if err != nil {
					log.WithError(err).Fatal("Failed to initialise telemetry")
				}
			} else {
				log.Debug("Telemetry is disabled")
			}

			operator, err := operator.NewOperator(ctx, &operator.Config{
				PollInterval:   viper.GetInt("pollInterval"),
				BatchSize:      viper.GetInt("batchSize"),
				DatabaseUrl:    viper.GetString("dbUrl"),
				BrokerUrls:     viper.GetString("brokers"),
				Topics:         viper.GetStringSlice("topics"),
				CompletionMode: completionMode,
			})
			if err != nil {
				log.WithError(err).Fatal("Failed to create operator")
			}

			// Channel to receive os signals
			sigChan := make(chan os.Signal, 1)
			// Channel to wait for graceful shutdown
			shutdownChan := make(chan struct{})
			signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

			go func() {
				defer close(shutdownChan)

				log.Info("Polling for events...")
				for {
					select {
					default:
						err := operator.Execute(ctx)
						if err != nil {
							log.WithError(err).Error("Operator failed")
						}
					case <-sigChan:
						log.Info("Stopping gox..")
						operator.Close(ctx)
						telemetry.Close()
						return
					}
				}
			}()

			<-sigChan
			close(sigChan)
			<-shutdownChan
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().IntVar(&pollInterval, "pollInterval", 100, "Poll interval in milliseconds.")
	rootCmd.PersistentFlags().IntVar(&batchSize, "batchSize", 50, "Interval batch size.")
	rootCmd.PersistentFlags().StringVar(&dbUrl, "db", "", "Database connection url (required).")
	rootCmd.PersistentFlags().StringVar(&brokerUrls, "brokers", "", "Comma-separated broker urls (required).")
	rootCmd.PersistentFlags().StringSliceVar(&topics, "topics", []string{}, "Comma-separated topics (required).")
	rootCmd.PersistentFlags().StringVar(&logLevel, "logLevel", "INFO", "Log level.")
	rootCmd.PersistentFlags().StringVar(&logFormat, "logFormat", "text", "Log format. Available options are 'json' and 'text'.")
	rootCmd.PersistentFlags().BoolVar(&enableTelemetry, "enableTelemetry", false, "Enable OTEL telemetry via OTLP (HTTP).")
	rootCmd.PersistentFlags().StringVar(&completionMode, "completionMode", "UPDATE", "Batch completion mode. Available options are 'UPDATE' and 'DELETE'")

	viper.BindPFlag("pollInterval", rootCmd.PersistentFlags().Lookup("pollInterval"))
	viper.BindPFlag("batchSize", rootCmd.PersistentFlags().Lookup("batchSize"))
	viper.BindPFlag("dbUrl", rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("brokers", rootCmd.PersistentFlags().Lookup("brokers"))
	viper.BindPFlag("topics", rootCmd.PersistentFlags().Lookup("topics"))
	viper.BindPFlag("logLevel", rootCmd.PersistentFlags().Lookup("logLevel"))
	viper.BindPFlag("logFormat", rootCmd.PersistentFlags().Lookup("logFormat"))
	viper.BindPFlag("enableTelemetry", rootCmd.PersistentFlags().Lookup("enableTelemetry"))
	viper.BindPFlag("completionMode", rootCmd.PersistentFlags().Lookup("completionMode"))
}

func initConfig() {
	viper.BindEnv("pollInterval", "GOX_POLL_INTERVAL")
	viper.BindEnv("batchSize", "GOX_BATCH_SIZE")
	viper.BindEnv("dbUrl", "GOX_DB_URL")
	viper.BindEnv("brokers", "GOX_BROKERS")
	viper.BindEnv("topics", "GOX_TOPICS")
	viper.BindEnv("logLevel", "GOX_LOG_LEVEL")
	viper.BindEnv("logFormat", "GOX_LOG_FORMAT")
	viper.BindEnv("enableTelemetry", "GOX_ENABLE_TELEMETRY")
	viper.BindEnv("completionMode", "GOX_COMPLETION_MODE")
}

func validateConfig() {
	for _, requiredParam := range requiredParameters {
		if !viper.IsSet(requiredParam.name) {
			log.Fatalf("Parameter '%s' was not provided. Populate the environment variable '%s' or start gox with the flag '--%s' set",
				requiredParam.name, requiredParam.environmentVariable, requiredParam.flag)
		}
	}
}

func parseLogLevel(logLevelStr string) log.Level {
	logLevel, err := log.ParseLevel(logLevelStr)
	if err != nil {
		log.Warnf("Failed to parse parameter '%s' to a log level, defaulting to warn", logLevelStr)
		logLevel = log.WarnLevel
	}

	return logLevel
}

func parseLogFormat(logFormatStr string) log.Formatter {
	var formatter log.Formatter

	switch strings.ToLower(logFormatStr) {
	case "json":
		formatter = &log.JSONFormatter{}
	case "text":
		fallthrough
	default:
		formatter = &log.TextFormatter{
			FullTimestamp: true,
		}
	}

	return formatter
}

func parseCompletionMode(completionModeStr string) operator.CompletionMode {
	var completionMode operator.CompletionMode

	switch strings.ToLower(completionModeStr) {
	case "update":
		completionMode = operator.UpdateCompletionMode
	case "delete":
		completionMode = operator.DeleteCompletionMode
	default:
		log.Warnf("Failed to parse parameter '%s' to a completion mode, defaulting to UPDATE", completionModeStr)
		completionMode = operator.UpdateCompletionMode
	}

	return completionMode
}
