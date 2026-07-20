package nodes

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Prateek-Gupta001/AgenticDAGWorkflow/store"
)

type Executor interface {
	Run(ctx context.Context, runId string, workflowId string, input map[string]any) error
	dispatchReady(ctx context.Context, runId string) error
	executeNode(ctx context.Context, runId string, nodeType NodeType, inputMap map[string]any, wg *sync.WaitGroup)
	SubmitApproval(ctx context.Context, runId string, decision string) error
	GetNodeStates(ctx context.Context, runId string) (map[string]store.NodeState, error)
	GetResult(ctx context.Context, runId string) (found bool, status string, output map[string]any, err error)
	GetApprovalState(ctx context.Context, runId string) (found bool, status string, input map[string]any, err error)
	GetNodeStatus(ctx context.Context, runId, nodeId string) (found bool, status string, err error)
	SubmitRetry(ctx context.Context, runId, nodeId string) error
	ListRecentRuns(ctx context.Context, limit int) ([]store.RunSummary, error)
	GetNodeEvents(ctx context.Context, runId, nodeId string) ([]store.NodeEvent, error)
}

type CustomerSupportExecutor struct {
	store *store.Store
	nodes map[NodeType]Node
}

func NewCustomerSupportExecutor(s *store.Store) *CustomerSupportExecutor {
	return &CustomerSupportExecutor{
		store: s,
		nodes: map[NodeType]Node{
			Input:             &InputNode{},
			Classify:          &ClassifyNode{},
			FetchCustomer:     &FetchCustomerNode{},
			FetchAccount:      &FetchAccountNode{},
			ChoosePath:        &ChoosePathNode{},
			CreateLinearIssue: &CreateLinearIssueNode{},
			CheckInvoice:      &CheckInvoiceNode{},
			HumanApproval:     &HumanApprovalNode{},
			DraftReplyBug:     &DraftReplyBugNode{},
			DraftReplyBilling: &DraftReplyBillingNode{},
			DraftReplyUnclear: &DraftReplyUnclearNode{},
		},
	}
}

func (e *CustomerSupportExecutor) Run(ctx context.Context, runId string, workflowId string, input map[string]any) error {

	runId, err := e.store.CreateRun(ctx, runId, workflowId, input, AllNodes)
	if err != nil {
		return err
	}
	// Input has no dependencies to inherit from, so it's the one node whose
	// input can't come from GetInputMap — nothing has completed yet. Run it
	// directly, through the same three calls dispatchReady would use, then
	// let the generic loop take over from here.
	if err := e.store.MarkAsRunning(ctx, runId, string(Input), input); err != nil {
		return err
	}
	output, err := e.nodes[Input].Execute(ctx, input)
	if err != nil {
		return e.store.MarkAsFailed(ctx, runId, string(Input), input, err.Error())
	}
	if err := e.store.SaveNodeProgress(ctx, runId, string(Input), input, output); err != nil {
		return err
	}

	return e.dispatchReady(ctx, runId)
}

func (e *CustomerSupportExecutor) dispatchReady(ctx context.Context, runId string) error {
	for {
		nodeStates, err := e.store.GetNodeStates(ctx, runId)

		if err != nil {
			return err
		}
		ready := NodesReadyForExecution(nodeStates, Deps, e.nodes)
		if len(ready) == 0 {
			return e.finalizeRun(ctx, runId, nodeStates)
		}

		var wg sync.WaitGroup
		for _, node := range ready {
			nodeType := node.Type()
			state := nodeStates[string(nodeType)]
			inputMap := GetInputMap(nodeStates)

			if nodeType == HumanApproval {
				// Park it. Don't call Execute, don't wg.Add — an external
				// event (the approve/reject call) resumes this later.
				if err := e.store.MarkAwaitingApproval(ctx, runId, string(nodeType), inputMap); err != nil {
					return err
				}
				continue
			}
			//THIS IS FOR TESTING RETRIALS.
			inputMap["_attempt"] = state.AttemptCount + 1 // the attempt about to happen
			if err := e.store.MarkAsRunning(ctx, runId, string(nodeType), inputMap); err != nil {
				return err
			}
			wg.Add(1)
			go e.executeNode(ctx, runId, nodeType, inputMap, &wg)
		}
		wg.Wait()
	}
}

func (e *CustomerSupportExecutor) executeNode(ctx context.Context, runId string, nodeType NodeType, inputMap map[string]any, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		if r := recover(); r != nil {
			if err := e.store.MarkAsFailed(ctx, runId, string(nodeType), inputMap, fmt.Sprintf("panic: %v", r)); err != nil {
				slog.Error(fmt.Sprintf("run %s node %s: failed to persist panic: %v", runId, nodeType, err))
			}
		}
	}()

	output, err := e.nodes[nodeType].Execute(ctx, inputMap)
	if err != nil {
		if storeErr := e.store.MarkAsFailed(ctx, runId, string(nodeType), inputMap, err.Error()); storeErr != nil {
			slog.Error(fmt.Sprintf("run %s node %s: failed to persist failure %v: %v", runId, nodeType, err, storeErr))
		}
		return
	}

	if err := e.store.SaveNodeProgress(ctx, runId, string(nodeType), inputMap, output); err != nil {
		slog.Error(fmt.Sprintf("run %s node %s: failed to persist success: %v", runId, nodeType, err))
		return
	}

	if nodeType == ChoosePath {
		branch, ok := output["branch"].(string)
		if !ok {
			e.store.MarkAsFailed(ctx, runId, string(nodeType), inputMap, "choose_path output missing branch")
			return
		}
		if err := e.store.MarkAsSkipped(ctx, runId, computeSkipSet(branch, Deps)); err != nil {
			slog.Error(fmt.Sprintf("run %s: failed to mark skip set: %v", runId, err))
		}
	}
}

func (e *CustomerSupportExecutor) SubmitApproval(ctx context.Context, runId string, decision string) error {
	nodeStates, err := e.store.GetNodeStates(ctx, runId)
	if err != nil {
		return err
	}
	state, ok := nodeStates[string(HumanApproval)]
	if !ok || state.Status != "awaiting_approval" {
		return fmt.Errorf("human_approval is not awaiting approval (status=%s)", state.Status)
	}

	inputMap := GetInputMap(nodeStates)
	inputMap["humanDecision"] = decision

	if err := e.store.MarkAsRunning(ctx, runId, string(HumanApproval), inputMap); err != nil {
		return err
	}

	output, err := e.nodes[HumanApproval].Execute(ctx, inputMap)
	if err != nil {
		return e.store.MarkAsFailed(ctx, runId, string(HumanApproval), inputMap, err.Error())
	}
	if err := e.store.SaveNodeProgress(ctx, runId, string(HumanApproval), inputMap, output); err != nil {
		return err
	}

	return e.dispatchReady(ctx, runId)
}

func (e *CustomerSupportExecutor) GetNodeStates(ctx context.Context, runId string) (map[string]store.NodeState, error) {
	return e.store.GetNodeStates(ctx, runId)
}

// GetResult reports whether a run has reached a terminal state. found=false
// means the run_id doesn't exist at all — distinct from "still running",
// which the handler needs to return 404 vs 200 correctly.
func (e *CustomerSupportExecutor) GetResult(ctx context.Context, runId string) (found bool, status string, output map[string]any, err error) {
	states, err := e.store.GetNodeStates(ctx, runId)
	if err != nil {
		return false, "", nil, err
	}
	if len(states) == 0 {
		return false, "", nil, nil
	}
	for _, t := range TerminalNodes {
		if state, ok := states[string(t)]; ok && state.Status == "completed" {
			return true, "completed", state.Output, nil
		}
	}
	for _, state := range states {
		if state.Status == "failed" {
			return true, "failed", nil, nil
		}
	}
	//todo: Is this like all right? What if it's pending or whatever? Ig it's alright.
	return true, "running", nil, nil
}

func (e *CustomerSupportExecutor) GetApprovalState(ctx context.Context, runId string) (found bool, status string, input map[string]any, err error) {
	states, err := e.store.GetNodeStates(ctx, runId)
	if err != nil {
		return false, "", nil, err
	}
	if len(states) == 0 {
		return false, "", nil, nil
	}
	state := states[string(HumanApproval)] // row always exists — CreateRun seeds every node in AllNodes as 'pending'
	return true, state.Status, state.Input, nil
}

func (e *CustomerSupportExecutor) SubmitRetry(ctx context.Context, runId, nodeId string) error {
	reset, err := e.store.ResetToPending(ctx, runId, nodeId)
	if err != nil {
		return err
	}
	if !reset {
		return nil // already retried elsewhere — not an error, just nothing to do
	}
	return e.dispatchReady(ctx, runId)
}

// finalizeRun runs once dispatchReady's loop hits a fixed point — nothing
// left pending-and-ready. Called from the one place that already knows the
// run is either done, blocked on approval, or genuinely stuck.
func (e *CustomerSupportExecutor) finalizeRun(ctx context.Context, runId string, nodeStates map[string]store.NodeState) error {
	for _, t := range TerminalNodes {
		if state, ok := nodeStates[string(t)]; ok && state.Status == "completed" {
			return e.store.CompleteRun(ctx, runId, state.Output)
		}
	}
	for _, state := range nodeStates {
		if state.Status == "failed" {
			return e.store.FailRun(ctx, runId)
		}
	}
	return nil // parked on human_approval — workflow_runs.status correctly stays 'running'
}

func (e *CustomerSupportExecutor) ListRecentRuns(ctx context.Context, limit int) ([]store.RunSummary, error) {
	return e.store.ListRecentRuns(ctx, limit)
}

func (e *CustomerSupportExecutor) GetNodeEvents(ctx context.Context, runId, nodeId string) ([]store.NodeEvent, error) {
	return e.store.GetNodeEvents(ctx, runId, nodeId)
}
