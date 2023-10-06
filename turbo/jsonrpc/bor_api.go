package jsonrpc

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/rpc"
)

// BorAPI Bor specific routines
type BorAPI interface {
	// Bor snapshot related (see ./bor_snapshot.go)
	GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error)
	GetAuthor(blockNrOrHash *rpc.BlockNumberOrHash) (*common.Address, error)
	GetSnapshotAtHash(hash common.Hash) (*Snapshot, error)
	GetSigners(number *rpc.BlockNumber) ([]common.Address, error)
	GetSignersAtHash(hash common.Hash) ([]common.Address, error)
	GetCurrentProposer() (common.Address, error)
	GetCurrentValidators() ([]*valset.Validator, error)
	GetSnapshotProposer(blockNrOrHash *rpc.BlockNumberOrHash) (common.Address, error)
	GetSnapshotProposerSequence(blockNrOrHash *rpc.BlockNumberOrHash) (BlockSigners, error)
	GetRootHash(start uint64, end uint64) (string, error)
}

// BorImpl is implementation of the BorAPI interface
type BorImpl struct {
	*BaseAPI
	db kv.RoDB // the chain db
}

// NewBorAPI returns BorImpl instance
func NewBorAPI(base *BaseAPI, db kv.RoDB) *BorImpl {
	return &BorImpl{
		BaseAPI: base,
		db:      db,
	}
}

func (api *BorImpl) bor() (*bor.Bor, error) {
	type lazy interface {
		HasEngine() bool
		Engine() consensus.EngineReader
	}

	switch engine := api.engine().(type) {
	case *bor.Bor:
		return engine, nil
	case lazy:
		if engine.HasEngine() {
			if bor, ok := engine.Engine().(*bor.Bor); ok {
				return bor, nil
			}
		}
	}

	return nil, fmt.Errorf("unknown or invalid consensus engine: %T", api.engine())
}
