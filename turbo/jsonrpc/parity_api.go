package jsonrpc

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"

	"github.com/ledgerwatch/erigon/rpc"
)

var latestTag = libcommon.BytesToHash([]byte("latest"))

var ErrWrongTag = fmt.Errorf("listStorageKeys wrong block tag or number: must be '%s' ('latest')", latestTag)

// ParityAPI the interface for the parity_ RPC commands
type ParityAPI interface {
	ListStorageKeys(ctx context.Context, account libcommon.Address, quantity int, offset *hexutility.Bytes, blockNumber rpc.BlockNumberOrHash) ([]hexutility.Bytes, error)
}

// ParityAPIImpl data structure to store things needed for parity_ commands
type ParityAPIImpl struct {
	*BaseAPI
	db kv.RoDB
}

// NewParityAPIImpl returns ParityAPIImpl instance
func NewParityAPIImpl(base *BaseAPI, db kv.RoDB) *ParityAPIImpl {
	return &ParityAPIImpl{
		BaseAPI: base,
		db:      db,
	}
}

// ListStorageKeys implements parity_listStorageKeys. Returns all storage keys of the given address
func (api *ParityAPIImpl) ListStorageKeys(ctx context.Context, account libcommon.Address, quantity int, offset *hexutility.Bytes, blockNumberOrTag rpc.BlockNumberOrHash) ([]hexutility.Bytes, error) {
	if err := api.checkBlockNumber(blockNumberOrTag); err != nil {
		return nil, err
	}
	keys := make([]hexutility.Bytes, 0)

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("listStorageKeys cannot open tx: %w", err)
	}
	defer tx.Rollback()
	a, err := rpchelper.NewLatestStateReader(tx).ReadAccountData(account)
	if err != nil {
		return nil, err
	} else if a == nil {
		return nil, fmt.Errorf("acc not found")
	}

	bn := rawdb.ReadCurrentBlockNumber(tx)
	minTxNum, err := rawdbv3.TxNums.Min(tx, *bn)
	if err != nil {
		return nil, err
	}

	from := account[:]
	if offset != nil {
		from = append(from, *offset...)
	}
	to, _ := kv.NextSubtree(account[:])
	r, err := tx.(kv.TemporalTx).DomainRange(kv.StorageDomain, from, to, minTxNum, order.Asc, quantity)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for r.HasNext() {
		k, _, err := r.Next()
		if err != nil {
			return nil, err
		}
		keys = append(keys, libcommon.CopyBytes(k[20:]))
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
