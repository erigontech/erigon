package jsonrpc

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/rpc"
)

// BorAPI Bor specific routines
type BorAPI interface {
	// Bor snapshot related (see ./bor_snapshot.go)
	GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error)
	GetAuthor(number *rpc.BlockNumber) (*common.Address, error)
	GetSnapshotAtHash(hash common.Hash) (*Snapshot, error)
	GetSigners(number *rpc.BlockNumber) ([]common.Address, error)
	GetSignersAtHash(hash common.Hash) ([]common.Address, error)
	GetCurrentProposer() (common.Address, error)
	GetCurrentValidators() ([]*valset.Validator, error)
	GetSnapshotProposerSequence(blockNrOrHash *rpc.BlockNumberOrHash) (BlockSigners, error)
	GetRootHash(start uint64, end uint64) (string, error)
}

// BorImpl is implementation of the BorAPI interface
type BorImpl struct {
	*BaseAPI
	db    kv.RoDB // the chain db
	borDb kv.RoDB // the consensus db
}

// NewBorAPI returns BorImpl instance
func NewBorAPI(base *BaseAPI, db kv.RoDB, borDb kv.RoDB) *BorImpl {
	return &BorImpl{
		BaseAPI: base,
		db:      db,
		borDb:   borDb,
	}
}
