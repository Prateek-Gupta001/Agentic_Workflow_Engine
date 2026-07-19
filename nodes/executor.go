package nodes

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Prateek-Gupta001/libraAIAssignment/store"
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
			return nil // fixed point — either done, or parked on an approval
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

	output, err := e.nodes[HumanApproval].Execute(ctx, inputMap)
	if err != nil {
		return e.store.MarkAsFailed(ctx, runId, string(HumanApproval), inputMap, err.Error())
	}
	if err := e.store.SaveNodeProgress(ctx, runId, string(HumanApproval), inputMap, output); err != nil {
		return err
	}

	return e.dispatchReady(ctx, runId) // resumes — DraftReplyUnclear is now unblocked
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

var TerminalNodes = []NodeType{DraftReplyBug, DraftReplyBilling, DraftReplyUnclear}

func GetInputMap(nodeStates map[string]store.NodeState) map[string]any {
	merged := make(map[string]any)
	for _, state := range nodeStates {
		if state.Status != "completed" {
			continue
		}
		for k, v := range state.Output {
			merged[k] = v
		}
	}
	return merged
}

// TODO: This has to be done at every node execution pass .. can this be optimized?
func NodesReadyForExecution(
	nodeStates map[string]store.NodeState,
	deps map[NodeType][]NodeType,
	registry map[NodeType]Node,
) []Node {
	var ready []Node
	for nodeType, dependencies := range deps {
		state, exists := nodeStates[string(nodeType)]
		if !exists || state.Status != "pending" {
			continue
		}
		allDepsCompleted := true
		for _, dep := range dependencies {
			depState, ok := nodeStates[string(dep)]
			if !ok || depState.Status != "completed" {
				allDepsCompleted = false
				break
			}
		}
		if allDepsCompleted {
			ready = append(ready, registry[nodeType])
		}
	}
	return ready
}

var branchRoot = map[string]NodeType{
	"bug":     CreateLinearIssue,
	"billing": CheckInvoice,
	"unclear": HumanApproval,
}

// computeSkipSet returns every node that should be marked skipped given the
// chosen branch: the two branch roots not taken, plus everything that
// transitively depends on them.
func computeSkipSet(chosenBranch string, deps map[NodeType][]NodeType) []string {
	skip := make(map[NodeType]bool)
	for branch, nodeType := range branchRoot {
		if branch != chosenBranch {
			skip[nodeType] = true
		}
	}
	for {
		added := false
		for nodeType, dependencies := range deps {
			if skip[nodeType] {
				continue
			}
			for _, dep := range dependencies {
				if skip[dep] {
					skip[nodeType] = true
					added = true
					break
				}
			}
		}
		if !added {
			break
		}
	}
	result := make([]string, 0, len(skip))
	for nodeType := range skip {
		result = append(result, string(nodeType))
	}
	return result
}

func (e *CustomerSupportExecutor) GetNodeStatus(ctx context.Context, runId, nodeId string) (found bool, status string, err error) {
	nodeStates, err := e.store.GetNodeStates(ctx, runId)
	if err != nil {
		return false, "", err
	}
	state, ok := nodeStates[nodeId]
	if !ok {
		return false, "", nil
	}
	return true, state.Status, nil
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
