package commands

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
)

const (
	keyLength = common.AddressLength + common.IncarnationLength
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
	seekBytes := strconv.AppendUint(account.Bytes(), a.Incarnation, 16)

	c, err := tx.CursorDupSort(kv.PlainState)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	keys := make([]hexutil.Bytes, 0)
	var k []byte

	if offset != nil {
		k, err = c.SeekBothRange(seekBytes, *offset)
	} else {
		k, _, err = c.Seek(seekBytes)
	}
	if err != nil {
		return nil, err
	}

	for k != nil && len(keys) != quantity {
		if !bytes.HasPrefix(k, seekBytes) {
			break
		}
		if len(k) <= keyLength {
			continue
		}
		keys = append(keys, k[keyLength:])

		k, _, err = c.Next()
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}
