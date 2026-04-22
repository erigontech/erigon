package epbs

import (
	"context"

	"github.com/erigontech/erigon/common"
)

// Signer abstracts BLS signing operations for the ePBS builder.
// Implementations may load keys from local files, remote KMS, etc.
type Signer interface {
	// Pubkey returns the builder's compressed BLS public key.
	Pubkey() common.Bytes48

	// SignBid signs a bid's signing root.
	SignBid(ctx context.Context, signingRoot common.Hash) (common.Bytes96, error)

	// SignEnvelope signs an execution payload envelope's signing root.
	SignEnvelope(ctx context.Context, signingRoot common.Hash) (common.Bytes96, error)

	// SignDeposit signs a builder deposit's signing root.
	SignDeposit(ctx context.Context, signingRoot common.Hash) (common.Bytes96, error)
}
