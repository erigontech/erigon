package stagedsync

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type IndexUnwinder interface {
	UnwindAddress(tx kv.RwTx, addr common.Address, idx uint64) error
	Dispose() error
}
