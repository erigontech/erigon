package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

type TgAPI interface {
	// System related (see ./tg_system.go)
	Forks(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (Forks, error)

	// Blocks related (see ./tg_blocks.go)
	GetHeaderByNumber(_ context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetHeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error)

	// Receipt related (see ./tg_receipts.go)
	GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
}

// TgImpl is implementation of the TgAPI interface
type TgImpl struct {
	db       ethdb.KV
	dbReader ethdb.Database
}

// NewTgAPI returns TgImpl instance
func NewTgAPI(db ethdb.KV, dbReader ethdb.Database) *TgImpl {
	return &TgImpl{
		db:       db,
		dbReader: dbReader,
	}
}
