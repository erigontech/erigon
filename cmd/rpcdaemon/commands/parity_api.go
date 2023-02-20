package commands

import (
	"context"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/rpc"
)

var latestTag = libcommon.BytesToHash([]byte("latest"))

var ErrWrongTag = fmt.Errorf("listStorageKeys wrong block tag or number: must be '%s' ('latest')", latestTag)

// ParityAPI the interface for the parity_ RPC commands
type ParityAPI interface {
	ListStorageKeys(ctx context.Context, account libcommon.Address, quantity int, offset *hexutil.Bytes, blockNumber rpc.BlockNumberOrHash) ([]hexutil.Bytes, error)
}

// ParityAPIImpl data structure to store things needed for parity_ commands
type ParityAPIImpl struct {
	db kv.RoDB
}

// NewParityAPIImpl returns ParityAPIImpl instance
func NewParityAPIImpl(db kv.RoDB) *ParityAPIImpl {
	return &ParityAPIImpl{
		db: db,
	}
}

// ListStorageKeys implements parity_listStorageKeys. Returns all storage keys of the given address
func (api *ParityAPIImpl) ListStorageKeys(ctx context.Context, account libcommon.Address, quantity int, offset *hexutil.Bytes, blockNumberOrTag rpc.BlockNumberOrHash) ([]hexutil.Bytes, error) {
	if err := api.checkBlockNumber(blockNumberOrTag); err != nil {
		return nil, err
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("listStorageKeys cannot open tx: %w", err)
	}
	defer tx.Rollback()
	a, err := state.NewPlainStateReader(tx).ReadAccountData(account)
	if err != nil {
		return nil, err
	} else if a == nil {
		return nil, fmt.Errorf("acc not found")
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, a.GetIncarnation())
	seekBytes := append(account.Bytes(), b...)

	c, err := tx.CursorDupSort(kv.PlainState)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	keys := make([]hexutil.Bytes, 0)
	var v []byte
	var seekVal []byte
	if offset != nil {
		seekVal = *offset
	}

	for v, err = c.SeekBothRange(seekBytes, seekVal); v != nil && len(keys) != quantity && err == nil; _, v, err = c.NextDup() {
		if len(v) > length.Hash {
			keys = append(keys, v[:length.Hash])
		} else {
			keys = append(keys, v)
		}
	}
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (api *ParityAPIImpl) checkBlockNumber(blockNumber rpc.BlockNumberOrHash) error {
	num, isNum := blockNumber.Number()
	if isNum && rpc.LatestBlockNumber == num {
		return nil
	}
	return ErrWrongTag
}
