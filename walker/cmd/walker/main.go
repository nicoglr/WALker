package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"4gclinical.com/walker/internal/config"
	"4gclinical.com/walker/internal/replication"
	"4gclinical.com/walker/internal/sink"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	slog.Info("WALker starting",
		"slot", cfg.Slot,
		"tables", cfg.Tables,
		"redis", cfg.RedisAddr,
	)

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer rdb.Close()

	s := sink.New(rdb, cfg.StreamPrefix, cfg.DB)
	runner := replication.New(cfg.PGDSN, cfg.Slot, cfg.Tables, s, cfg.StatusInterval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runner.Run(ctx); err != nil {
		slog.Error("runner exited", "err", err)
		os.Exit(1)
	}
}
