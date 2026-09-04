package sszql

import (
	"context"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// note: derived types of Proof and Leaf can change later

type SSZQLAPI interface {
	GetExecutionBlock(ctx context.Context, bnh rpc.BlockNumberOrHash) (*types.Block, error)
}

type Path string

type Filter string

type Anchor string

type Gindex uint64

type Proof string

type Leaf string

type Result string

type SSZQLImpl struct {
	DB          kv.RoDB
	BlockReader dbservices.FullBlockReader
}

type ResolvedPath struct {
	Gindex Gindex
	Leaf   Leaf
	Value  Result
}

type Alias struct {
	Anchor Anchor `json:"anchor"`
	Path   Path   `json:"path"`
	Filter Filter `json:"filter,omitempty"`
	Alias  string `json:"alias"`
}

type SSZQuery struct {
	Anchor    Anchor `json:"anchor"`
	Path      Path   `json:"path"`
	Filter    Filter `json:"filter,omitempty"`
	Summaries bool   `json:"summaries,omitempty"`
}

type SSZQLRequest struct {
	Aliases       []Alias    `json:"aliases,omitempty"`
	Queries       []SSZQuery `json:"queries"`
	IncludeProofs bool       `json:"include_proofs,omitempty"`
	Multiproof    bool       `json:"multiproof,omitempty"`
}

type AliasResponse struct {
	Alias string `json:"alias"`
	Value string `json:"value"`
}

type SSZQLResponse struct {
	Aliases  []AliasResponse `json:"aliases,omitempty"`
	Paths    []Path          `json:"paths"`
	Gindices []Gindex        `json:"gindices"`
	Leaves   []Leaf          `json:"leaves"`
	Results  []Result        `json:"results"`
	Proofs   []Proof         `json:"proofs,omitempty"`
}
