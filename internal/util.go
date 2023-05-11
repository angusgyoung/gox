package internal

import (
	"log"
	"os"
	"strconv"
	"strings"
)

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
