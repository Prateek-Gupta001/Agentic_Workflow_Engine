package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Prateek-Gupta001/libraAIAssignment/nodes"
)

//Now it's time to code up the API server.

type DAGWorkflow struct {
	executor   nodes.Executor
	listenAddr string
}

func NewDAGWorkflow(executor nodes.Executor, listenAddr string) *DAGWorkflow {
	return &DAGWorkflow{
		executor:   executor,
		listenAddr: listenAddr,
	}
}

func (s *DAGWorkflow) Run(ctx context.Context, stop context.CancelFunc) (err error) {
	defer stop()
	// go func() {
	// 	slog.Info("Pprof attached: Pprof server running on localhost:6060")
	// 	// "nil" tells it to use the DefaultServeMux where pprof registered itself
	// 	if err := http.ListenAndServe("localhost:6060", nil); err != nil {
	// 		slog.Error("Pprof failed", "error", err)
	// 	}
	// }()
	r := s.newHTTPHandler()
	srv := &http.Server{
		Addr:         s.listenAddr,
		ReadTimeout:  time.Second * 5,
		WriteTimeout: time.Second * 30,
		Handler:      r,
	}
	srvErr := make(chan error, 1)
	go func() {
		slog.Info("Running HTTP server...")
		srvErr <- srv.ListenAndServe()
	}()
	select {
	case err := <-srvErr:
		return err
	case <-ctx.Done():
		stop()
	}
	slog.Info("Graceful Shutdown in progress!")
	timeCtx, _ := context.WithTimeout(context.Background(), time.Second*20)
	if err := srv.Shutdown(timeCtx); err != nil {
		slog.Info("got this error while doing graceful shutdown", "error", err)
		return err
	}

	slog.Info("Graceful shutdown successful!")
	return nil

}

func (s *DAGWorkflow) newHTTPHandler() *http.ServeMux {
	r := http.NewServeMux()
	// r.HandleFunc("POST /chat", convertToHandleFunc((s.Chat)))
	// r.HandleFunc("GET /stats", convertToHandleFunc(s.GetCostSaved))
	// r.HandleFunc("GET /health", convertToHandleFunc(s.HealthCheck))
	return r
}

type apiFunc func(w http.ResponseWriter, r *http.Request) *APIError

type APIError struct {
	Error   error
	Message string //don't wanna send the user/hacker at the frontend .. anything that they might wanna know ... like the error itself
	Status  int    //hence send a custom message right then and there ...
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func convertToHandleFunc(f apiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiError := f(w, r)
		if apiError != nil {
			slog.Error("Got this error from an http handler func", "error", apiError.Error)
			WriteJSON(w, apiError.Status, struct{ Error string }{Error: apiError.Message})
		}
	}
}
