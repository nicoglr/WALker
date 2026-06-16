package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/walker/internal/config"
)

func TestDefaults(t *testing.T) {
	t.Setenv("WALKER_PG_DSN", "")
	t.Setenv("WALKER_TABLES", "")
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_REDIS_ADDR", "")
	t.Setenv("WALKER_STREAM_PREFIX", "")
	t.Setenv("WALKER_STATUS_INTERVAL", "")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "postgres://postgres:postgres@localhost:5432/mydb", cfg.PGDSN)
	// Slot is derived from instance ID: "test-instance" → "test_instance"
	assert.Equal(t, "walker_slot_test_instance", cfg.Slot)
	assert.Len(t, cfg.Tables, 2)
	assert.Equal(t, "test-instance", cfg.InstanceID)
	assert.Equal(t, "localhost:6380", cfg.RedisAddr)
	assert.Equal(t, "cdc", cfg.StreamPrefix)
	assert.Equal(t, 10*time.Second, cfg.StatusInterval)
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "my-instance")
	t.Setenv("WALKER_TABLES", "public.foo,public.bar,public.baz")
	t.Setenv("WALKER_STATUS_INTERVAL", "30s")

	cfg, err := config.Load()
	require.NoError(t, err)

	// Slot derived from instance ID
	assert.Equal(t, "walker_slot_my_instance", cfg.Slot)
	assert.Len(t, cfg.Tables, 3)
	assert.Equal(t, 30*time.Second, cfg.StatusInterval)
}

func TestSlotDerivedFromInstanceID(t *testing.T) {
	cases := []struct {
		name       string
		instanceID string
		wantSlot   string
	}{
		{"simple", "myinstance", "walker_slot_myinstance"},
		{"hyphen", "my-instance", "walker_slot_my_instance"},
		{"uppercase", "MyInstance", "walker_slot_myinstance"},
		{"mixed special", "My-Instance.v2", "walker_slot_my_instance_v2"},
		{"digits", "inst123", "walker_slot_inst123"},
		{"leading underscore", "_inst", "walker_slot__inst"},
		{"non-ascii", "my-ärger", "walker_slot_my__rger"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WALKER_INSTANCE_ID", tc.instanceID)
			cfg, err := config.Load()
			require.NoError(t, err)
			assert.Equal(t, tc.wantSlot, cfg.Slot)
		})
	}
}

func TestSlotTooLong(t *testing.T) {
	// "walker_slot_" is 12 chars; 52-char instance ID → 64 chars → over limit
	longID := strings.Repeat("a", 52)
	t.Setenv("WALKER_INSTANCE_ID", longID)
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrInvalidSlot)
}

func TestSlotAtMaxLength(t *testing.T) {
	// "walker_slot_" (12) + 51 'a's = 63 chars — exactly at the Postgres limit
	maxID := strings.Repeat("a", 51)
	t.Setenv("WALKER_INSTANCE_ID", maxID)
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Len(t, cfg.Slot, 63)
}

func TestDegenerateInstanceID(t *testing.T) {
	// All-special-char IDs sanitize to all underscores → rejected
	t.Setenv("WALKER_INSTANCE_ID", "---")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrInvalidValue)
}

func TestMissingInstanceID(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingField)
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
	require.ErrorIs(t, err, config.ErrInvalidValue)
}

func TestStatusIntervalBelowMinimum(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_STATUS_INTERVAL", "1s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBelowMinimum)
}

func TestEmptyTablesEntry(t *testing.T) {
	t.Setenv("WALKER_INSTANCE_ID", "test-instance")
	t.Setenv("WALKER_TABLES", "public.foo,,public.bar")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrEmptyTableEntry)
}
