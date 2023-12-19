package sync

import (
	"github.com/ledgerwatch/erigon/core/types"
)

type dbLayer interface {
	WriteBlocks([]*types.Block) error
}
