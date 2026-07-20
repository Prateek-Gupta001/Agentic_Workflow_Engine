package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Prateek-Gupta001/libraAIAssignment/nodes"
	"github.com/google/uuid"
)

// Now it's time to code up the API server.
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

type RunSummaryResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Request   string    `json:"request"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *DAGWorkflow) ListRuns(w http.ResponseWriter, r *http.Request) *APIError {
	runs, err := s.executor.ListRecentRuns(r.Context(), 10)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later.",
		}
	}
	resp := make([]RunSummaryResponse, 0, len(runs))
	for _, run := range runs {
		resp = append(resp, RunSummaryResponse{ID: run.ID, Status: run.Status, Request: run.Request, CreatedAt: run.CreatedAt})
	}
	WriteJSON(w, http.StatusOK, resp)
	return nil
}

func (s *DAGWorkflow) Run(ctx context.Context, stop context.CancelFunc) (err error) {
	defer stop()

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

func (s *DAGWorkflow) newHTTPHandler() http.Handler {
	r := http.NewServeMux()
	r.HandleFunc("POST /v1/req", convertToHandleFunc(s.SubmitRequest))
	r.HandleFunc("GET /v1/runs/{id}/state", convertToHandleFunc(s.GetNodeStates))
	r.HandleFunc("GET /v1/runs/{id}/result", convertToHandleFunc(s.PollRequest))
	r.HandleFunc("GET /v1/runs/{id}/approval", convertToHandleFunc(s.PollHumanApprovalNode))
	r.HandleFunc("POST /v1/runs/{id}/approval", convertToHandleFunc(s.SubmittingHumanApproval))
	r.HandleFunc("POST /v1/runs/{id}/nodes/{nodeId}/retry", convertToHandleFunc(s.RetryNode))
	r.HandleFunc("GET /v1/runs", convertToHandleFunc(s.ListRuns))
	r.HandleFunc("GET /v1/runs/{id}/nodes/{nodeId}/events", convertToHandleFunc(s.GetNodeEvents))

	return CorsMiddleware(RunContextMiddleware(r))
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

type SubmitRequestStruct struct {
	Request    string `json:"req"`
	AccountId  string `json:"accountId"`
	CustomerId string `json:"customerId"`
}

// This is the main function that gives the request to the DAG executor.
func (s *DAGWorkflow) SubmitRequest(w http.ResponseWriter, r *http.Request) *APIError {
	//1. Intialise map with request.
	//2. Pass it to the executor for it to run.
	var reqBody SubmitRequestStruct

	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&reqBody); err != nil {
		// Replace this with however your APIError struct is initialized
		return &APIError{
			Status:  http.StatusBadRequest,
			Error:   err,
			Message: "Failed to parse request body",
		}
	}
	defer r.Body.Close()
	id := uuid.NewString()
	slog.Info("Got a request", "request", reqBody.Request)
	inputMap := make(map[string]any)
	inputMap["request"] = reqBody.Request
	inputMap["accountId"] = reqBody.AccountId
	inputMap["customerId"] = reqBody.CustomerId

	slog.Debug("INPUT MAP REQUEST", "request", inputMap["request"])
	ctx := context.WithoutCancel(r.Context())
	err := s.executor.Run(ctx, id, "customer_support_v1", inputMap)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later. ",
		}
	}

	WriteJSON(w, http.StatusOK, SubmitRequestResponse{
		ReqId: id,
	})
	return nil
}

type SubmitRequestResponse struct {
	ReqId string `json:"req_id"`
}

//What other API handlers do we need?
//1. One for submitting the request. DONE ABOVE
//2. One for polling on the request. Is it done yet? Is it done yet?
//3. One for polling on the human approval node thing. Is human approval required?
//4. One for polling all the node_states periodically to see their statuses.
//5. One for submitting human approval and moving e.SubmitApproval() forward.

func (s *DAGWorkflow) GetNodeStates(w http.ResponseWriter, r *http.Request) *APIError {
	runId := r.PathValue("id")
	if runId == "" {
		return &APIError{Status: http.StatusBadRequest, Message: "run id is required"}
	}

	states, err := s.executor.GetNodeStates(r.Context(), runId)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later.",
		}
	}
	// GetNodeStates returns an empty map with no error for an unknown run_id
	// — the SQL query just matches zero rows. That's the difference between
	// "not found" and "found but not started" you need to check explicitly.
	if len(states) == 0 {
		return &APIError{Status: http.StatusNotFound, Message: "run not found"}
	}

	WriteJSON(w, http.StatusOK, states)
	return nil
}

type PollRequestResponse struct {
	Status string         `json:"status"`
	Output map[string]any `json:"output,omitempty"`
}

func (s *DAGWorkflow) PollRequest(w http.ResponseWriter, r *http.Request) *APIError {
	runId := r.PathValue("id")
	if runId == "" {
		return &APIError{Status: http.StatusBadRequest, Message: "run id is required"}
	}

	found, status, output, err := s.executor.GetResult(r.Context(), runId)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later.",
		}
	}
	if !found {
		return &APIError{Status: http.StatusNotFound, Message: "run not found"}
	}

	WriteJSON(w, http.StatusOK, PollRequestResponse{Status: status, Output: output})
	return nil
}

type ApprovalStateResponse struct {
	Status           string         `json:"status"`
	RequiresApproval bool           `json:"requires_approval"`
	Input            map[string]any `json:"input,omitempty"`
}

func (s *DAGWorkflow) PollHumanApprovalNode(w http.ResponseWriter, r *http.Request) *APIError {
	runId := r.PathValue("id")
	if runId == "" {
		return &APIError{Status: http.StatusBadRequest, Message: "run id is required"}
	}

	found, status, input, err := s.executor.GetApprovalState(r.Context(), runId)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later.",
		}
	}
	if !found {
		return &APIError{Status: http.StatusNotFound, Message: "run not found"}
	}

	WriteJSON(w, http.StatusOK, ApprovalStateResponse{
		Status:           status,
		RequiresApproval: status == "awaiting_approval",
		Input:            input,
	})
	return nil
}

type SubmitApprovalRequest struct {
	Decision string `json:"decision"`
}

func (s *DAGWorkflow) SubmittingHumanApproval(w http.ResponseWriter, r *http.Request) *APIError {
	runId := r.PathValue("id")
	if runId == "" {
		return &APIError{Status: http.StatusBadRequest, Message: "run id is required"}
	}

	var reqBody SubmitApprovalRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&reqBody); err != nil {
		return &APIError{
			Status:  http.StatusBadRequest,
			Error:   err,
			Message: "Failed to parse request body",
		}
	}
	defer r.Body.Close()

	if reqBody.Decision != "approved" && reqBody.Decision != "rejected" {
		return &APIError{
			Status:  http.StatusBadRequest,
			Message: `decision must be "approved" or "rejected"`,
		}
	}

	// Same reasoning as SubmitRequest: don't block this response on
	// dispatchReady finishing. The client finds out via PollRequest.
	ctx := context.WithoutCancel(r.Context())
	go func() {
		if err := s.executor.SubmitApproval(ctx, runId, reqBody.Decision); err != nil {
			slog.Error("submit approval failed", "run_id", runId, "error", err)
		}
	}()

	WriteJSON(w, http.StatusAccepted, HumanApprovalSubmitted{
		Message: "Approval Submitted",
	})
	return nil
}

type HumanApprovalSubmitted struct {
	Message string `json:"message"`
}

func (s *DAGWorkflow) RetryNode(w http.ResponseWriter, r *http.Request) *APIError {
	runId := r.PathValue("id")
	nodeId := r.PathValue("nodeId")
	if runId == "" || nodeId == "" {
		return &APIError{Status: http.StatusBadRequest, Message: "run id and node id are required"}
	}

	found, status, err := s.executor.GetNodeStatus(r.Context(), runId, nodeId)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later.",
		}
	}
	if !found {
		return &APIError{Status: http.StatusNotFound, Message: "node not found"}
	}
	if status != "failed" {
		return &APIError{Status: http.StatusConflict, Message: fmt.Sprintf("node is not failed (status=%s)", status)}
	}

	ctx := context.WithoutCancel(r.Context())
	go func() {
		if err := s.executor.SubmitRetry(ctx, runId, nodeId); err != nil {
			slog.Error("retry failed", "run_id", runId, "node_id", nodeId, "error", err)
		}
	}()

	WriteJSON(w, http.StatusAccepted, struct{}{})
	return nil
}

type NodeEventResponse struct {
	Status       string    `json:"status"`
	Message      string    `json:"message"`
	AttemptCount int       `json:"attempt_count"`
	CreatedAt    time.Time `json:"created_at"`
}

func (s *DAGWorkflow) GetNodeEvents(w http.ResponseWriter, r *http.Request) *APIError {
	runId := r.PathValue("id")
	nodeId := r.PathValue("nodeId")
	if runId == "" || nodeId == "" {
		return &APIError{Status: http.StatusBadRequest, Message: "run id and node id are required"}
	}

	events, err := s.executor.GetNodeEvents(r.Context(), runId, nodeId)
	if err != nil {
		return &APIError{
			Status:  http.StatusInternalServerError,
			Error:   err,
			Message: "We are experiencing some difficulties right now. Please try again later.",
		}
	}

	resp := make([]NodeEventResponse, 0, len(events))
	for _, e := range events {
		resp = append(resp, NodeEventResponse{Status: e.Status, Message: e.Message, AttemptCount: e.AttemptCount, CreatedAt: e.CreatedAt})
	}
	WriteJSON(w, http.StatusOK, resp)
	return nil
}
