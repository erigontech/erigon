package graph

import (
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/turbo/jsonrpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	GraphQLAPI  jsonrpc.GraphQLAPI
	db          kv.RoDB
	filters     *rpchelper.Filters
	blockReader services.FullBlockReader
}
