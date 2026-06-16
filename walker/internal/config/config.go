package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
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

// sanitizeForSlot converts an arbitrary string into a valid Postgres replication
// slot name suffix: lowercase ASCII, with every character outside [a-z0-9_]
// (including non-ASCII Unicode) replaced by an underscore.
func sanitizeForSlot(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
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

	instanceID := getenv("WALKER_INSTANCE_ID", "")
	if instanceID == "" {
		return Config{}, fmt.Errorf("WALKER_INSTANCE_ID must be set: %w", ErrMissingField)
	}
	sanitized := sanitizeForSlot(instanceID)
	if strings.Trim(sanitized, "_") == "" {
		return Config{}, fmt.Errorf("WALKER_INSTANCE_ID %q produces an empty slot suffix after sanitization: %w", instanceID, ErrInvalidValue)
	}
	slot := "walker_slot_" + sanitized
	if len(slot) > 63 {
		return Config{}, fmt.Errorf("derived slot name %q exceeds Postgres 63-character limit: %w", slot, ErrInvalidSlot)
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
