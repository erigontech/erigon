package transactions

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type EnrichedTransaction struct {
	types.Transaction
	idx            int
	isBorStateSync bool
	// borStateSyncHash uses a pointer to be memory efficient in the case of this
	// not being a bor state sync txn (empty common.Hash takes 32 bytes)
	borStateSyncHash *common.Hash
}

func (et *EnrichedTransaction) Hash() common.Hash {
	if et.IsBorStateSync() {
		return *et.borStateSyncHash
	}

	return et.Transaction.Hash()
}

func (et *EnrichedTransaction) AsMessage(s types.Signer, baseFee *big.Int, rules *chain.Rules) (types.Message, error) {
	if et.IsBorStateSync() {
		// we use an empty message for bor state sync txn since it gets handled differently
		return types.Message{}, nil
	}

	return et.Transaction.AsMessage(s, baseFee, rules)
}

func (et *EnrichedTransaction) Idx() int {
	return et.idx
}

func (et *EnrichedTransaction) IsBorStateSync() bool {
	return et.isBorStateSync
}

func AllBlockTransactions(
	ctx context.Context,
	block *types.Block,
	chainConfig *chain.Config,
	blockReader services.FullBlockReader,
	tx kv.Tx,
) ([]*EnrichedTransaction, error) {
	res := make([]*EnrichedTransaction, 0, block.Transactions().Len()+1)
	for _, txn := range block.Transactions() {
		res = append(res, &EnrichedTransaction{
			Transaction: txn,
		})
	}

	if chainConfig.Bor == nil {
		return res, nil
	}

	events, err := blockReader.EventsByBlock(ctx, tx, block.Hash(), block.NumberU64())
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return res, nil
	}

	borStateSyncTransactionHash := types.ComputeBorTxHash(block.NumberU64(), block.Hash())
	res = append(res, &EnrichedTransaction{
		Transaction:      types.NewBorTransaction(),
		isBorStateSync:   true,
		borStateSyncHash: &borStateSyncTransactionHash,
	})

	return res, nil
}

func FindTransactionInBlock(
	ctx context.Context,
	txnHash common.Hash,
	block *types.Block,
	chainConfig *chain.Config,
	blockReader services.FullBlockReader,
	tx kv.Tx,
) (*EnrichedTransaction, error) {
	txns, err := AllBlockTransactions(ctx, block, chainConfig, blockReader, tx)
	if err != nil {
		return nil, err
	}

	for _, txn := range txns {
		if txnHash == txn.Hash() {
			return txn, nil
		}
	}

	return nil, fmt.Errorf("transaction %#x not found", txnHash)
}
