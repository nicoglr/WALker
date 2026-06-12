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
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_REDIS_ADDR", "")
	t.Setenv("WALKER_STREAM_PREFIX", "")
	t.Setenv("WALKER_STATUS_INTERVAL", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PGDSN != "postgres://postgres:postgres@localhost:5432/mydb" {
		t.Errorf("unexpected PGDSN: %s", cfg.PGDSN)
	}
	if cfg.Slot != "walker_slot" {
		t.Errorf("unexpected Slot: %s", cfg.Slot)
	}
	if len(cfg.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(cfg.Tables))
	}
	if cfg.InstanceID != "test-instance" {
		t.Errorf("unexpected InstanceID: %s", cfg.InstanceID)
	}
	if cfg.RedisAddr != "localhost:6380" {
		t.Errorf("unexpected RedisAddr: %s", cfg.RedisAddr)
	}
	if cfg.StreamPrefix != "cdc" {
		t.Errorf("unexpected StreamPrefix: %s", cfg.StreamPrefix)
	}
	if cfg.StatusInterval != 10*time.Second {
		t.Errorf("unexpected StatusInterval: %v", cfg.StatusInterval)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "my-instance")
	t.Setenv("WALKER_SLOT", "my_slot")
	t.Setenv("WALKER_TABLES", "public.foo,public.bar,public.baz")
	t.Setenv("WALKER_STATUS_INTERVAL", "30s")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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

func TestMissingInstanceID(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "")
	_, err := config.Load()
	if err == nil {
		t.Error("expected error for missing WALKER_INSTANCE_ID, got nil")
	}
}

func TestStreamPrefixTrailingDot(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "my-instance")
	t.Setenv("WALKER_STREAM_PREFIX", "cdc.")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.StreamPrefix != "cdc" {
		t.Errorf("expected trailing dot stripped, got %q", cfg.StreamPrefix)
	}
}

func TestInvalidStatusInterval(t *testing.T) {
	t.Setenv("WALKER_STATUS_INTERVAL", "1hour") // wrong format
	_, err := config.Load()
	if err == nil {
		t.Error("expected error for invalid duration, got nil")
	}
}

func TestStatusIntervalBelowMinimum(t *testing.T) {
	t.Setenv("WALKER_STATUS_INTERVAL", "1s") // below 10s minimum
	_, err := config.Load()
	if err == nil {
		t.Error("expected error for sub-minimum interval, got nil")
	}
}

func TestInvalidSlotName(t *testing.T) {
	cases := []struct {
		name string
		slot string
	}{
		{"uppercase", "MySlot"},
		{"hyphen", "my-slot"},
		{"space", "my slot"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WALKER_SLOT", tc.slot)
			_, err := config.Load()
			if err == nil {
				t.Errorf("expected error for slot %q, got nil", tc.slot)
			}
		})
	}
}

func TestEmptyTablesEntry(t *testing.T) {
	t.Setenv("WALKER_TABLES", "public.foo,,public.bar") // double comma → empty entry
	_, err := config.Load()
	if err == nil {
		t.Error("expected error for empty table entry, got nil")
	}
}
