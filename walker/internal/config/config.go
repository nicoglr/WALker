package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	PGDSN          string
	Slot           string
	Tables         []string
	DB             string
	RedisAddr      string
	StreamPrefix   string
	StatusInterval time.Duration
}

func Load() Config {
	interval, err := time.ParseDuration(getenv("WALKER_STATUS_INTERVAL", "10s"))
	if err != nil {
		interval = 10 * time.Second
	}
	rawTables := getenv("WALKER_TABLES", "public.orders,public.products")
	tables := strings.Split(rawTables, ",")
	for i := range tables {
		tables[i] = strings.TrimSpace(tables[i])
	}
	return Config{
		PGDSN:          getenv("WALKER_PG_DSN", "postgres://postgres:postgres@localhost:5432/mydb"),
		Slot:           getenv("WALKER_SLOT", "walker_slot"),
		Tables:         tables,
		DB:             getenv("WALKER_DB", "mydb"),
		RedisAddr:      getenv("WALKER_REDIS_ADDR", "localhost:6380"),
		StreamPrefix:   getenv("WALKER_STREAM_PREFIX", "cdc."),
		StatusInterval: interval,
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
