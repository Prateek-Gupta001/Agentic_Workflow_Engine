package nodes

// this contains the dependency map.
// This will be a read-only map and would need to make it so that the reads don't require a mutex .. because that would be additional complexity.
var Deps = map[NodeType][]NodeType{
	Input:             {},
	Classify:          {Input},
	FetchCustomer:     {Input},
	FetchAccount:      {Input},
	ChoosePath:        {Classify, FetchCustomer, FetchAccount},
	CreateLinearIssue: {ChoosePath},
	CheckInvoice:      {ChoosePath},
	HumanApproval:     {ChoosePath},
	DraftReplyBug:     {CreateLinearIssue},
	DraftReplyBilling: {CheckInvoice},
	DraftReplyUnclear: {HumanApproval},
}
