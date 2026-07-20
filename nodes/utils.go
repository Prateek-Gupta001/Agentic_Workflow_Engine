package nodes

import (
	"context"
	"log/slog"
	"strings"

	"github.com/Prateek-Gupta001/AgenticDAGWorkflow/store"
)

func MockLLM(input string) string {
	input = strings.ToLower(input)

	switch {
	case strings.Contains(input, "bug"),
		strings.Contains(input, "error"),
		strings.Contains(input, "crash"),
		strings.Contains(input, "broken"):
		return "bug"

	case strings.Contains(input, "bill"),
		strings.Contains(input, "invoice"),
		strings.Contains(input, "payment"),
		strings.Contains(input, "charge"):
		return "billing"

	default:
		return "unclear"
	}
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
	slog.Info("For this chosen branch", "branch", chosenBranch, "the skip set computed is this", result)
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
