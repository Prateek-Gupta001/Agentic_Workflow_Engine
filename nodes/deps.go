package nodes

// this contains the dependency map.
// maps in Go are safe for concurrent reads
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
