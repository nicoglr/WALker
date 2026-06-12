package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)

	assert.Equal(t, "postgres://postgres:postgres@localhost:5432/mydb", cfg.PGDSN)
	assert.Equal(t, "walker_slot", cfg.Slot)
	assert.Len(t, cfg.Tables, 2)
	assert.Equal(t, "test-instance", cfg.InstanceID)
	assert.Equal(t, "localhost:6380", cfg.RedisAddr)
	assert.Equal(t, "cdc", cfg.StreamPrefix)
	assert.Equal(t, 10*time.Second, cfg.StatusInterval)
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "my-instance")
	t.Setenv("WALKER_SLOT", "my_slot")
	t.Setenv("WALKER_TABLES", "public.foo,public.bar,public.baz")
	t.Setenv("WALKER_STATUS_INTERVAL", "30s")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "my_slot", cfg.Slot)
	assert.Len(t, cfg.Tables, 3)
	assert.Equal(t, 30*time.Second, cfg.StatusInterval)
}

func TestMissingInstanceID(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "")
	_, err := config.Load()
	require.ErrorContains(t, err, "WALKER_INSTANCE_ID")
}

func TestStreamPrefixTrailingDot(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "my-instance")
	t.Setenv("WALKER_STREAM_PREFIX", "cdc.")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "cdc", cfg.StreamPrefix)
}

func TestInvalidStatusInterval(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_STATUS_INTERVAL", "1hour")
	_, err := config.Load()
	require.ErrorContains(t, err, "WALKER_STATUS_INTERVAL")
}

func TestStatusIntervalBelowMinimum(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_STATUS_INTERVAL", "1s")
	_, err := config.Load()
	require.ErrorContains(t, err, "WALKER_STATUS_INTERVAL")
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
			t.Setenv("WALKER_INSTANCE_ID", "test-instance")
			t.Setenv("WALKER_SLOT", tc.slot)
			_, err := config.Load()
			require.ErrorContains(t, err, "WALKER_SLOT")
		})
	}
}

func TestEmptyTablesEntry(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_TABLES", "public.foo,,public.bar")
	_, err := config.Load()
	require.ErrorContains(t, err, "WALKER_TABLES")
}
