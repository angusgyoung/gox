package internal

import (
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func GetEnvString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return fallback
}

func GetReqEnvString(key string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	log.Fatalf("Environment variable '%s' must be set\n", key)
	return ""
}

func GetReqEnvStringList(key string) []string {
	if value, ok := os.LookupEnv(key); ok {
		return strings.Split(value, ",")
	}

	log.Fatalf("Environment variable '%s' must be set\n", key)
	return nil
}

func GetEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		strconv.Atoi(value)
	}
	return fallback
}
