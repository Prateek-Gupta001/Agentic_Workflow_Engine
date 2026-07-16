package nodes

import "github.com/Prateek-Gupta001/libraAIAssignment/store"

type Executor struct {
	store *store.Store
	nodes map[NodeType]Node
}
