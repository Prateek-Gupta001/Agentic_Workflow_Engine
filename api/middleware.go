package api

import (
	"context"
	"log/slog"
	"net/http"
)

// contextKey is a custom type to avoid context collision
type contextKey string

const loggerKey contextKey = "slog_logger"

// RunContextMiddleware intercepts the request, extracts the run ID,
// and injects a pre-configured logger into the context.
func RunContextMiddleware(next http.Handler) http.Handler {
	slog.Info("middle ware running!")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Start with the default logger
		logger := slog.Default()

		// Extract {id} from the path
		if runID := r.PathValue("id"); runID != "" {
			logger = logger.With(slog.String("run_id", runID))
		}

		// Bonus: Since you have a retry route with {nodeId}, extract that too!
		if nodeID := r.PathValue("nodeId"); nodeID != "" {
			logger = logger.With(slog.String("node_id", nodeID))
		}

		// Inject the scoped logger into the request context
		ctx := context.WithValue(r.Context(), loggerKey, logger)

		// Pass the new context down the chain
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetLogger is a utility for your handlers to pull the scoped logger out of the context.
func GetLogger(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return logger
	}
	// Fallback to default if middleware wasn't applied
	return slog.Default()
}
