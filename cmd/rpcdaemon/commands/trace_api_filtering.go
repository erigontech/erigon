package commands

import (
	"bytes"
	"context"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// Block Implements trace_block
func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// Get Implements trace_get
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64) (interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// Transaction Implements trace_transaction
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

func retrieveHistory(tx ethdb.Tx, addr *common.Address, fromBlock uint64, toBlock uint64) ([]uint64, error) {
	addrBytes := addr.Bytes()
	ca := tx.Cursor(dbutils.AccountsHistoryBucket).Prefix(addrBytes)
	var blockNumbers []uint64

	for k, v, err := ca.First(); k != nil; k, v, err = ca.Next() {
		if err != nil {
			return nil, err
		}

		numbers, _, err := dbutils.WrapHistoryIndex(v).Decode()
		if err != nil {
			return nil, err
		}

		blockNumbers = append(blockNumbers, numbers...)
	}

	// cleanup for invalid blocks
	start := -1
	end := -1
	for i, b := range blockNumbers {
		if b >= fromBlock && b <= toBlock && start == -1 {
			start = i
			continue
		}

		if b > toBlock {
			end = i
			break
		}
	}

	if start == -1 {
		return []uint64{}, nil
	}

	if end == -1 {
		return blockNumbers[start:], nil
	}
	// Remove dublicates
	return blockNumbers[start:end], nil
}

func isAddressInFilter(addr *common.Address, filter []*common.Address) bool {
	if filter == nil {
		return true
	}
	i := sort.Search(len(filter), func(i int) bool {
		return bytes.Equal(filter[i].Bytes(), addr.Bytes())
	})

	return i != len(filter)
}
