package config_test

import (
	"testing"
	"time"

	"4gclinical.com/walker/internal/config"
)

func TestDefaults(t *testing.T) {
	t.Setenv("WALKER_PG_DSN", "")
	t.Setenv("WALKER_SLOT", "")
	t.Setenv("WALKER_TABLES", "")
	t.Setenv("WALKER_DB", "")
	t.Setenv("WALKER_REDIS_ADDR", "")
	t.Setenv("WALKER_STREAM_PREFIX", "")
	t.Setenv("WALKER_STATUS_INTERVAL", "")

	cfg := config.Load()

	if cfg.PGDSN != "postgres://postgres:postgres@localhost:5432/mydb" {
		t.Errorf("unexpected PGDSN: %s", cfg.PGDSN)
	}
	if cfg.Slot != "walker_slot" {
		t.Errorf("unexpected Slot: %s", cfg.Slot)
	}
	if len(cfg.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(cfg.Tables))
	}
	if cfg.DB != "mydb" {
		t.Errorf("unexpected DB: %s", cfg.DB)
	}
	if cfg.RedisAddr != "localhost:6380" {
		t.Errorf("unexpected RedisAddr: %s", cfg.RedisAddr)
	}
	if cfg.StreamPrefix != "cdc." {
		t.Errorf("unexpected StreamPrefix: %s", cfg.StreamPrefix)
	}
	if cfg.StatusInterval != 10*time.Second {
		t.Errorf("unexpected StatusInterval: %v", cfg.StatusInterval)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("WALKER_SLOT", "my_slot")
	t.Setenv("WALKER_TABLES", "public.foo,public.bar,public.baz")
	t.Setenv("WALKER_STATUS_INTERVAL", "30s")

	cfg := config.Load()

	if cfg.Slot != "my_slot" {
		t.Errorf("expected my_slot, got %s", cfg.Slot)
	}
	if len(cfg.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(cfg.Tables))
	}
	if cfg.StatusInterval != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.StatusInterval)
	}
}
