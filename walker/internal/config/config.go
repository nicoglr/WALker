package config

import (
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)

// Row is a map of column name → value for a single CDC row.
// Values follow wal2json's natural JSON types; numeric/bigint values are
// carried as json.Number (exact decimal text) to avoid float64 rounding.
type Row map[string]any

// Config holds all runtime configuration, loaded from environment variables.
type Config struct {
	PGDSN          string
	Slot           string
	Tables         []string
	DB             string
	RedisAddr      string
	StreamPrefix   string
	StatusInterval time.Duration
}

const minStatusInterval = 10 * time.Second

// Load reads config from environment variables and validates it.
// Returns an error for any invalid or dangerous value.
func Load() (Config, error) {
	rawInterval := getenv("WALKER_STATUS_INTERVAL", "10s")
	interval, err := time.ParseDuration(rawInterval)
	if err != nil {
		return Config{}, fmt.Errorf("WALKER_STATUS_INTERVAL %q: %w", rawInterval, err)
	}
	if interval < minStatusInterval {
		return Config{}, fmt.Errorf(
			"WALKER_STATUS_INTERVAL %v is below minimum %v (would spam Postgres with status updates)",
			interval, minStatusInterval,
		)
	}

	rawTables := getenv("WALKER_TABLES", "public.orders,public.products")
	tables := strings.Split(rawTables, ",")
	for i := range tables {
		tables[i] = strings.TrimSpace(tables[i])
	}
	for _, t := range tables {
		if t == "" {
			return Config{}, fmt.Errorf("WALKER_TABLES %q contains an empty entry", rawTables)
		}
	}

	slot := getenv("WALKER_SLOT", "walker_slot")
	if err := validateSlotName(slot); err != nil {
		return Config{}, fmt.Errorf("WALKER_SLOT: %w", err)
	}

	return Config{
		PGDSN:          getenv("WALKER_PG_DSN", "postgres://postgres:postgres@localhost:5432/mydb"),
		Slot:           slot,
		Tables:         tables,
		DB:             getenv("WALKER_DB", "mydb"),
		RedisAddr:      getenv("WALKER_REDIS_ADDR", "localhost:6380"),
		StreamPrefix:   getenv("WALKER_STREAM_PREFIX", "cdc."),
		StatusInterval: interval,
	}, nil
}

// validateSlotName enforces Postgres slot name rules: lowercase letters, digits,
// and underscores only. This matches what Postgres itself accepts and prevents
// any SQL injection risk when the name is embedded in query strings.
func validateSlotName(s string) error {
	if s == "" {
		return fmt.Errorf("slot name must not be empty")
	}
	for _, r := range s {
		if !unicode.IsLower(r) && !unicode.IsDigit(r) && r != '_' {
			return fmt.Errorf("slot name %q contains invalid character %q (only [a-z0-9_] allowed)", s, r)
		}
	}
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
