package eladapter

import (
	"math/big"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// AssembledPayload contains the execution result needed to bid and reveal.
type AssembledPayload struct {
	Eth1Block      *cltypes.Eth1Block
	BlobsBundle    *BlobsBundle
	RequestsBundle *typesproto.RequestsBundle
	BlockValue     *big.Int
}

// BlobsBundle contains the blobs and proofs belonging to an assembled payload.
type BlobsBundle struct {
	Commitments [][]byte
	Proofs      [][]byte
	Blobs       [][]byte
}
