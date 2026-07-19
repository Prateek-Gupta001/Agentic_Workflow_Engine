package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/Prateek-Gupta001/libraAIAssignment/api"
	"github.com/Prateek-Gupta001/libraAIAssignment/nodes"
	"github.com/Prateek-Gupta001/libraAIAssignment/store"
	"github.com/joho/godotenv"
)

func main() {
	opts := returnOpts()
	err := godotenv.Load()
	if err != nil {
		slog.Error("got this error while trying to load a dotenv file", "error", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)
	store, err := store.NewStore()
	if err != nil {
		slog.Error("Got this error while trying to intialise a store", "error", err)
		panic(err)
	}
	executor := nodes.NewCustomerSupportExecutor(store)
	DAG := api.NewDAGWorkflow(executor, ":8080")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := DAG.Run(ctx, stop); err != nil {
		slog.Error("Got this error while running the server!", "error", err)
		panic(err)
	}
}

func returnOpts() *slog.HandlerOptions {
	return &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.String("time", a.Value.Time().Format("15:04:05"))
			}

			if a.Key == slog.SourceKey {
				source, _ := a.Value.Any().(*slog.Source)
				if source != nil {
					return slog.String("src", filepath.Base(source.File)+":"+strconv.Itoa(source.Line))
				}
			}
			return a
		},
	}

}
