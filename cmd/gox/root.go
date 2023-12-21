package gox

import (
	"context"
	"github.com/angusgyoung/gox/internal/operator"
	"github.com/angusgyoung/gox/internal/telemetry"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type requiredParameter struct {
	name               string
	flag               string
	environmetVariable string
}

var (
	pollInterval       int
	batchSize          int
	dbUrl              string
	brokerUrls         string
	topics             []string
	logLevel           string
	logFormat          string
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
			logLevelStr := viper.GetString("logLevel")
			logLevel, err := log.ParseLevel(logLevelStr)
			if err != nil {
				log.Warnf("Failed to parse parameter '%s' to a log level, defaulting to warn", logLevelStr)
				logLevel = log.WarnLevel
			}
			log.SetLevel(logLevel)

			logFormat := strings.ToLower(viper.GetString("logFormat"))
			switch logFormat {
			case "json":
				log.SetFormatter(&log.JSONFormatter{})
			case "text":
				fallthrough
			default:
				log.SetFormatter(&log.TextFormatter{
					FullTimestamp: true,
				})
			}

			log.Info("Starting gox...")

			validateConfig()
			ctx := context.Background()

			// Initialise telemetry
			telemetry.Initialise()

			operator, err := operator.NewOperator(ctx, &operator.OperatorConfig{
				PollInterval: viper.GetInt("pollInterval"),
				BatchSize:    viper.GetInt("batchSize"),
				DatabaseUrl:  viper.GetString("dbUrl"),
				BrokerUrls:   viper.GetString("brokers"),
				Topics:       viper.GetStringSlice("topics"),
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
	rootCmd.PersistentFlags().StringVar(&dbUrl, "db", "", "Database connection url (required)")
	rootCmd.PersistentFlags().StringVar(&brokerUrls, "brokers", "", "Comma-separated broker urls (required)")
	rootCmd.PersistentFlags().StringSliceVar(&topics, "topics", []string{}, "Comma-separated topics (required)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "logLevel", "WARN", "Log level.")
	rootCmd.PersistentFlags().StringVar(&logFormat, "logFormat", "text", "Log format. Available options are 'json' and 'text'.")

	viper.BindPFlag("pollInterval", rootCmd.PersistentFlags().Lookup("pollInterval"))
	viper.BindPFlag("batchSize", rootCmd.PersistentFlags().Lookup("batchSize"))
	viper.BindPFlag("dbUrl", rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("brokers", rootCmd.PersistentFlags().Lookup("brokers"))
	viper.BindPFlag("topics", rootCmd.PersistentFlags().Lookup("topics"))
	viper.BindPFlag("logLevel", rootCmd.PersistentFlags().Lookup("logLevel"))
	viper.BindPFlag("logFormat", rootCmd.PersistentFlags().Lookup("logFormat"))
}

func initConfig() {
	viper.SetEnvPrefix("gox")
	viper.AutomaticEnv()
	viper.BindEnv("dbUrl", "GOX_DB_URL")
	viper.BindEnv("logLevel", "GOX_LOG_LEVEL")
	viper.BindEnv("logFormat", "GOX_LOG_FORMAT")

	viper.SetDefault("logLevel", "INFO")
	viper.SetDefault("pollInterval", 100)
	viper.SetDefault("batchSize", 50)
}

func validateConfig() {
	for _, requiredParam := range requiredParameters {
		if !viper.IsSet(requiredParam.name) {
			log.Fatalf("Parameter '%s' was not provided. Populate the environment variable '%s' or start gox with the flag '--%s' set",
				requiredParam.name, requiredParam.environmetVariable, requiredParam.flag)
		}
	}
}
