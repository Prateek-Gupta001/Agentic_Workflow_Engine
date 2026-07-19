package nodes

import (
	"context"
	"errors"
	"fmt"
	"maps"
)

//This is going to define all the nodes that exist.
//What are the different types of Nodes that we have.

type NodeType string

const (
	Input             NodeType = "input"
	Classify          NodeType = "classify"
	FetchCustomer     NodeType = "fetch_customer"
	FetchAccount      NodeType = "fetch_account"
	ChoosePath        NodeType = "choose_path"
	CreateLinearIssue NodeType = "create_linear_issue"
	CheckInvoice      NodeType = "check_invoice"
	HumanApproval     NodeType = "human_approval"
	DraftReplyBug     NodeType = "draft_reply_bug"
	DraftReplyBilling NodeType = "draft_reply_billing"
	DraftReplyUnclear NodeType = "draft_reply_unclear"
)

type Node interface {
	Type() NodeType
	Execute(ctx context.Context, input map[string]any) (map[string]any, error) //this is the main execution function for Nodes.
}

var AllNodes = []string{
	string(Input),
	string(Classify),
	string(FetchCustomer),
	string(FetchAccount),
	string(ChoosePath),
	string(CreateLinearIssue),
	string(CheckInvoice),
	string(HumanApproval),
	string(DraftReplyBug),
	string(DraftReplyBilling),
	string(DraftReplyUnclear),
}

// For when a node is skipped.
var ErrNodeSkipped = errors.New("node skipped")

// Input Node.
type InputNode struct{}

func (i *InputNode) Type() NodeType { return Input }
func (i *InputNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	//need to validate that we got a "request" in the map.
	//need to put it in the map and return it.
	if _, ok := input["request"]; ok != true {
		return nil, errors.New("request field is required!")
	}
	out := maps.Clone(input)
	out["task"] = out["request"]

	return out, nil
}

type FetchCustomerNode struct{}

// TODO: Fetch data from postgres later on.
func (i *FetchCustomerNode) Type() NodeType { return FetchCustomer }
func (i *FetchCustomerNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	//need to validate that we got a "customerId" in the map.
	if _, ok := input["customerId"]; ok != true {
		return nil, errors.New("customerId field is required!")
	}
	out := maps.Clone(input)
	out["customerContext"] = out["customerContext"]

	return out, nil
}

type FetchAccountNode struct{}

// TODO: Fetch data from postgres later on.
func (i *FetchAccountNode) Type() NodeType { return FetchAccount }
func (i *FetchAccountNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	//need to validate that we got a "accoundId" in the map.
	if _, ok := input["accountId"]; ok != true {
		return nil, errors.New("accountId field is required!")
	}
	out := maps.Clone(input)
	out["accountContext"] = out["accountContext"]

	return out, nil
}

type ClassifyNode struct{}

func (i *ClassifyNode) Type() NodeType { return Classify }
func (i *ClassifyNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	//TODO: also need to add dependency checking here ... in all of these nodes.
	task, ok := input["task"].(string)
	if !ok {
		return nil, errors.New("task must be a string")
	}
	output := MockLLM(task)

	if output != "bug" && output != "billing" && output != "unclear" {
		//TODO: A log here is required.
		return nil, fmt.Errorf("LLM returned invalid classification: %q", output)
	}

	out := maps.Clone(input)
	out["task"] = output

	return out, nil
}

type ChoosePathNode struct{}

func (i *ChoosePathNode) Type() NodeType { return ChoosePath }
func (i *ChoosePathNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	//this node just chooses the path based on the Classify node.
	if _, ok := input["task"]; ok != true {
		return nil, errors.New("task field is required!")
	}
	out := maps.Clone(input)
	out["branch"] = input["task"]
	return out, nil
}

type CreateLinearIssueNode struct{}

func (I *CreateLinearIssueNode) Type() NodeType { return CreateLinearIssue }
func (n *CreateLinearIssueNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	text, ok := input["request"].(string)
	if !ok {
		return nil, errors.New("request field is required")
	}
	attempt, _ := input["_attempt"].(int)
	issue, err := mockCreateLinearIssue(text, attempt)
	if err != nil {
		return nil, err
	}
	out := maps.Clone(input)
	out["linearIssue"] = issue
	return out, nil
}

func mockCreateLinearIssue(text string, attempt int) (string, error) {
	if attempt < 2 {
		return "", errors.New("mock Linear API timeout")
	}
	return "MOCK-LINEAR-ISSUE-42", nil
}

type CheckInvoiceNode struct{}

func (I *CheckInvoiceNode) Type() NodeType { return CheckInvoice }
func (i *CheckInvoiceNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	branch, ok := input["branch"].(string)
	if !ok || branch != "billing" {
		//TODO: NEED A LOG HERE.
		return nil, ErrNodeSkipped //todo: maybe add a skip here?
	}
	out := maps.Clone(input)
	out["invoice"] = "MOCK-INVOICE" //TODO: Maybe get it from postgres too?
	return out, nil
}

type HumanApprovalNode struct {
}

func (n *HumanApprovalNode) Type() NodeType { return HumanApproval }
func (n *HumanApprovalNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	decision, ok := input["humanDecision"].(string)
	if !ok {
		return nil, errors.New("humanDecision field is required")
	}
	if decision != "approved" && decision != "rejected" {
		return nil, fmt.Errorf("humanDecision must be 'approved' or 'rejected', got %q", decision)
	}
	out := maps.Clone(input)
	out["humanDecision"] = decision
	return out, nil
}

type DraftReplyBugNode struct{}

func (n *DraftReplyBugNode) Type() NodeType { return DraftReplyBug }
func (n *DraftReplyBugNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	issue, ok := input["linearIssue"].(string)
	if !ok {
		return nil, errors.New("linearIssue field is required")
	}
	out := maps.Clone(input)
	out["reply"] = mockDraftReply(fmt.Sprintf("we filed %s to track this", issue))
	return out, nil
}

type DraftReplyBillingNode struct{}

func (n *DraftReplyBillingNode) Type() NodeType { return DraftReplyBilling }
func (n *DraftReplyBillingNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	invoice, ok := input["invoice"].(map[string]any)
	if !ok {
		return nil, errors.New("invoice field is required")
	}
	out := maps.Clone(input)
	out["reply"] = mockDraftReply(fmt.Sprintf("invoice %v is %v", invoice["invoiceId"], invoice["status"]))
	return out, nil
}

type DraftReplyUnclearNode struct{}

func (n *DraftReplyUnclearNode) Type() NodeType { return DraftReplyUnclear }
func (n *DraftReplyUnclearNode) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	decision, ok := input["humanDecision"].(string) //TODO: You can play on this one maybe? for checking whether or not this node can be executed or not..
	if !ok {
		return nil, errors.New("humanDecision field is required")
	}
	out := maps.Clone(input)
	out["reply"] = mockDraftReply(fmt.Sprintf("per human review: %s", decision))
	return out, nil
}

func mockDraftReply(context string) string {
	return fmt.Sprintf("Thanks for reaching out — here's what we found: %s", context)
}
