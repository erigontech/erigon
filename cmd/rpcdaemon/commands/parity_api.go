package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

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
	var k []byte

	if offset != nil {
		s := append(seekBytes, *offset...)
		k, _, err = c.Seek(s)
	} else {
		k, _, err = c.Seek(seekBytes)
	}
	if err != nil {
		return nil, err
	}
	maxCount, err := c.CountDuplicates()
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < maxCount && k != nil && len(keys) != quantity; i++ {
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
