package commands

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
)

// ParityAPI the interface for the parity_ RPC commands
type ParityAPI interface {
	ListStorageKeys(ctx context.Context, account common.Address, quantity int, offset *hexutil.Bytes) ([]hexutil.Bytes, error)
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
func (api *ParityAPIImpl) ListStorageKeys(ctx context.Context, account common.Address, quantity int, offset *hexutil.Bytes) ([]hexutil.Bytes, error) {
	tx, txErr := api.db.BeginRo(ctx)
	if txErr != nil {
		return nil, fmt.Errorf("listStorageKeys cannot open tx: %w", txErr)
	}
	defer tx.Rollback()
	a, err := state.NewPlainStateReader(tx).ReadAccountData(account)
	if err != nil {
		return nil, err
	} else if a == nil {
		return nil, fmt.Errorf("acc not found")
	}
	c, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return nil, err
	}
	keys := make([]hexutil.Bytes, 0)
	var (
		k []byte
		e error
	)
	if offset != nil {
		k, _, e = c.SeekExact(*offset)
	} else {
		k, _, e = c.Seek(account.Bytes())
	}
	for ; k != nil && e == nil && len(keys) != quantity; k, _, e = c.Next() {
		if e != nil {
			return nil, e
		}
		if !bytes.HasPrefix(k, account.Bytes()) {
			break
		}
		keys = append(keys, k)
	}
	return keys, nil
}
