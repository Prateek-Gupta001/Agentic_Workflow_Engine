package nodes

import (
	"context"
	"fmt"

	"github.com/Prateek-Gupta001/libraAIAssignment/store"
)

type Executor struct {
	store *store.Store
	nodes map[NodeType]Node
}

func NewExecutor(s *store.Store) *Executor {
	return &Executor{
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

// Now let's write the main executor loop for this DAG workflow.
func (e *Executor) Run(ctx context.Context, workflowId string, input map[string]any, nodeIDs []string) error {
	runId, err := e.store.CreateRun(ctx, "customer_support_v1", input, AllNodes)
	if err != nil {
		return err
	}
	//Now we need to execute the nodes.
	//How do we do that?
	//Well .. we can look at the current state of nodes to see which all are pending.
	//Given their pending/not pending states + the MAP state + you can find two things:
	//The current input map + WHICH NODE(S) executed.
	//Once you find those .. execute them! using go routines.
	for {
		nodeStates, err := e.store.GetNodeStates(ctx, runId)
		if err != nil {
			return nil
		}
		inputMap := GetInputMap(nodeStates)
		NodesToBeExecuted := NodesReadyForExecution(nodeStates, Deps, e.nodes)
		if len(NodesToBeExecuted) == 0 {
			break
		}
		for _, node := range NodesToBeExecuted {
			err := e.store.MarkAsRunning(ctx, runId, string(node.Type()), inputMap)
			if err != nil {
				err := e.store.MarkAsFailed(ctx, runId, string(node.Type()), inputMap, err.Error())
				if err != nil {
					return err
				}
				return err
			}
			output, err := node.Execute(ctx, inputMap)
			if err != nil {
				err := e.store.MarkAsFailed(ctx, runId, string(node.Type()), inputMap, err.Error())
				if err != nil {
					return err
				}
				return err
			}
			//how do we get the ouptut OF THE NODE THAT JUST EXECUTED. currently output contains the full map.
			//we could range over the map maybe?
			err = e.store.SaveNodeProgress(ctx, runId, string(node.Type()), inputMap, output)
			if node.Type() == ChoosePath {
				branch, ok := output["branch"].(string)
				if !ok {
					return fmt.Errorf("choose_path output missing branch")
				}
				if err := e.store.MarkAsSkipped(ctx, runId, computeSkipSet(branch, Deps)); err != nil {
					return err
				}
			}
			if err != nil {
				return err
			}
		}
	}
	//A few things... we don't have parallel execution yet. We would need to fire off go routines for that.
	//Also handle the skipping thing. If a nodes get skipped then all the nodes that depends upon that node should get skipped.
	//Only those nodes are ready for execution whose dependency nodes (the nodes THAT node depends upon) have the status as
	//'completed'
	return nil

}

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
