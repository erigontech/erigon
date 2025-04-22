package jsonrpc

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/rawdb"

	ethereum "github.com/erigontech/erigon"
)

// TaikoAPI is the interface for the taiko_* RPC commands.
type TaikoAPI interface {
	// HeadL1Origin returns the latest L2 block's corresponding L1 origin.
	HeadL1Origin() (*rawdb.L1Origin, error)

	// L1OriginByID returns the L2 block's corresponding L1 origin.
	L1OriginByID(blockID *math.HexOrDecimal256) (*rawdb.L1Origin, error)

	// GetSyncMode returns the node sync mode.
	GetSyncMode() (string, error)
}

type TaikoAPIImpl struct {
	db kv.TemporalRoDB
}

func NewTaikoAPIImpl(db kv.TemporalRoDB) *TaikoAPIImpl {
	return &TaikoAPIImpl{
		db: db,
	}
}

// HeadL1Origin returns the latest L2 block's corresponding L1 origin.
func (t *TaikoAPIImpl) HeadL1Origin() (*rawdb.L1Origin, error) {
	tx, err := t.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockID, err := rawdb.ReadHeadL1Origin(tx)
	if err != nil {
		return nil, err
	}

	if blockID == nil {
		return nil, ethereum.NotFound
	}

	l1Origin, err := rawdb.ReadL1Origin(tx, blockID)
	if err != nil {
		return nil, err
	}

	if l1Origin == nil {
		return nil, ethereum.NotFound
	}

	return l1Origin, nil
}

// L1OriginByID returns the L2 block's corresponding L1 origin.
func (t *TaikoAPIImpl) L1OriginByID(blockID *math.HexOrDecimal256) (*rawdb.L1Origin, error) {
	tx, err := t.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	l1Origin, err := rawdb.ReadL1Origin(tx, (*big.Int)(blockID))
	if err != nil {
		return nil, err
	}

	if l1Origin == nil {
		return nil, ethereum.NotFound
	}

	return l1Origin, nil
}

// GetSyncMode returns the node sync mode.
func (t *TaikoAPIImpl) GetSyncMode() (string, error) {
	return "full", nil //TODO change hardcoded value
}
