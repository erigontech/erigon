package graph

import (
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	GraphQLAPI jsonrpc.GraphQLAPI
}
