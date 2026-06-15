package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)

// Config holds all runtime configuration, loaded from environment variables.
type Config struct {
	PGDSN          string
	Slot           string
	Tables         []string
	InstanceID     string
	RedisAddr      string
	StreamPrefix   string
	StatusInterval time.Duration
}

// Sentinel errors returned by Load and validateSlotName.
// Callers may use errors.Is to identify specific failure kinds.
var (
	ErrMissingField    = errors.New("required field not set")
	ErrBelowMinimum    = errors.New("value below minimum")
	ErrInvalidSlot     = errors.New("invalid slot name")
	ErrEmptyTableEntry = errors.New("empty table entry")
	ErrInvalidValue    = errors.New("invalid value")
)

const minStatusInterval = 10 * time.Second

// Load reads config from environment variables and validates it.
// Returns an error for any invalid or dangerous value.
func Load() (Config, error) {
	rawInterval := getenv("WALKER_STATUS_INTERVAL", "10s")
	interval, err := time.ParseDuration(rawInterval)
	if err != nil {
		return Config{}, fmt.Errorf("WALKER_STATUS_INTERVAL %q: %w", rawInterval, fmt.Errorf("%w: %w", ErrInvalidValue, err))
	}
	if interval < minStatusInterval {
		return Config{}, fmt.Errorf(
			"WALKER_STATUS_INTERVAL %v is below minimum %v (would spam Postgres with status updates): %w",
			interval, minStatusInterval, ErrBelowMinimum,
		)
	}

	rawTables := getenv("WALKER_TABLES", "public.orders,public.products")
	tables := strings.Split(rawTables, ",")
	for i := range tables {
		tables[i] = strings.TrimSpace(tables[i])
	}
	for _, t := range tables {
		if t == "" {
			return Config{}, fmt.Errorf("WALKER_TABLES %q contains an empty entry: %w", rawTables, ErrEmptyTableEntry)
		}
	}

	slot := getenv("WALKER_SLOT", "walker_slot")
	if err := validateSlotName(slot); err != nil {
		return Config{}, fmt.Errorf("WALKER_SLOT: %w", err)
	}

	instanceID := getenv("WALKER_INSTANCE_ID", "")
	if instanceID == "" {
		return Config{}, fmt.Errorf("WALKER_INSTANCE_ID must be set: %w", ErrMissingField)
	}

	return Config{
		PGDSN:          getenv("WALKER_PG_DSN", "postgres://postgres:postgres@localhost:5432/mydb"),
		Slot:           slot,
		Tables:         tables,
		InstanceID:     instanceID,
		RedisAddr:      getenv("WALKER_REDIS_ADDR", "localhost:6380"),
		StreamPrefix:   strings.TrimSuffix(getenv("WALKER_STREAM_PREFIX", "cdc"), "."),
		StatusInterval: interval,
	}, nil
}

// validateSlotName enforces Postgres slot name rules: lowercase letters, digits,
// and underscores only. This matches what Postgres itself accepts and prevents
// any SQL injection risk when the name is embedded in query strings.
func validateSlotName(s string) error {
	if s == "" {
		return fmt.Errorf("slot name must not be empty: %w", ErrInvalidSlot)
	}
	for _, r := range s {
		if !unicode.IsLower(r) && !unicode.IsDigit(r) && r != '_' {
			return fmt.Errorf("slot name %q contains invalid character %q (only [a-z0-9_] allowed): %w", s, r, ErrInvalidSlot)
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
