// Package eladapter bridges the ePBS builder with the execution-layer block
// assembly API. It converts execution-layer types into CL-compatible types so
// that the epbs package only depends on execution/builder for Parameters.
package eladapter

import (
	"math/big"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// AssembledPayload holds the result of an EL block assembly,
// converted to CL-compatible types.
type AssembledPayload struct {
	Eth1Block      *cltypes.Eth1Block
	BlobsBundle    *BlobsBundle
	RequestsBundle *typesproto.RequestsBundle
	BlockValue     *big.Int
}

// BlobsBundle holds blob sidecar data in raw byte form.
type BlobsBundle struct {
	Commitments [][]byte
	Proofs      [][]byte
	Blobs       [][]byte
}
