package commands

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/rpc"
)

// BorAPI Bor specific routines
type BorAPI interface {
	// Bor snapshot related (see ./bor_snapshot.go)
	GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error)
	GetAuthor(number *rpc.BlockNumber) (*libcommon.Address, error)
	GetSnapshotAtHash(hash libcommon.Hash) (*Snapshot, error)
	GetSigners(number *rpc.BlockNumber) ([]libcommon.Address, error)
	GetSignersAtHash(hash libcommon.Hash) ([]libcommon.Address, error)
	GetCurrentProposer() (libcommon.Address, error)
	GetCurrentValidators() ([]*bor.Validator, error)
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
