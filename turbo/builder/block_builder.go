package builder

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
)

type BlockBuilderFunc func(tx kv.Tx, param *core.BlockBuilderParameters, interrupt *int32) (*types.Block, error)

// BlockBuilder builds Proof-of-Stake payloads (PoS "mining")
type BlockBuilder struct {
}

func NewBlockBuilder(ctx context.Context, tx kv.Tx, fn BlockBuilderFunc, param *core.BlockBuilderParameters) *BlockBuilder {
	// TODO: start a builder goroutine that stops on ctx.Done
	tx.Rollback()
	return &BlockBuilder{}
}

func (b *BlockBuilder) Stop() *types.Block {
	// TODO: implement idempotently
	return nil
}
