package epbs

import (
	"context"

	"github.com/erigontech/erigon/common"
)

// Signer signs builder bids and payload envelopes.
type Signer interface {
	Pubkey() common.Bytes48
	SignBid(context.Context, common.Hash) (common.Bytes96, error)
	SignEnvelope(context.Context, common.Hash) (common.Bytes96, error)
}
