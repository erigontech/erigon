package jsonrpc

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type TransactionListResult struct {
	BlocksSummary map[hexutil.Uint64]*BlockSummary `json:"blocksSummary"`
	Results       []*TransactionMatch              `json:"results"`
}

type TransactionMatch struct {
	Hash        common.Hash            `json:"hash"`
	Transaction *RPCTransaction        `json:"transaction"`
	Receipt     map[string]interface{} `json:"receipt"`
}

type transactionSearchResultMaterializer struct {
	api *Otterscan2APIImpl
}

func (m *transactionSearchResultMaterializer) Convert(ctx context.Context, tx kv.Tx, idx uint64) (*TransactionMatch, error) {
	txn, err := m.api._blockReader.TxnByTxId(ctx, tx, idx)
	if err != nil {
		return nil, err
	}

	blockNum, _, err := m.api.txnLookup(tx, txn.Hash())
	if err != nil {
		return nil, err
	}
	block, err := m.api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	receipt, err := m.api._getTransactionReceipt(ctx, tx, txn.Hash())
	if err != nil {
		return nil, err
	}

	result := &TransactionMatch{
		Hash:        txn.Hash(),
		Transaction: newRPCTransaction(txn, block.Hash(), blockNum, 0, block.BaseFee()),
		Receipt:     receipt,
	}
	return result, nil
}

// Implements a template method for API that expose an address-based search results,
// like ERC20 or ERC721 txs that contains transfers related to a certain address.
//
// Usually this method implements most part of the job, and caller methods just wrap
// it with corresponding DB tables.
//
// Semantics of corresponding parameters are the same in the caller methods, so it
// should be assumed this doc is the source of truth.
//
// The idx param is 0-based index of the first match that should be returned, considering
// the elements are numbered [0, numElem - 1].
//
// The count param determines the maximum of how many results should be returned. It may
// return less than count elements if the table's last record is reached and there are
// no more results available.
//
// Those 2 params allow for a flexible way to build paginated results, i.e., you can get the
// 3rd page of results in a 25 element page by passing: idx == (3 - 1) * 25, count == 25.
//
// Most likely, for a search results when the matches are shown backwards in time, and pages
// are dynamically numbered backwards from the last search results, getting the 3rd page
// would require the client code to use: idx == (totalMatches - 3 * 25), count == 25; the
// search results should then be reversed in the UI.
func (api *Otterscan2APIImpl) genericTransferList(ctx context.Context, addr common.Address, idx, count uint64, indexBucket, counterBucket string) (*TransactionListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var srm SearchResultMaterializer[TransactionMatch] = &transactionSearchResultMaterializer{api}
	ret, err := genericResultList(ctx, tx, addr, idx, count, indexBucket, counterBucket, srm)
	if err != nil {
		return nil, err
	}

	blocks := make([]hexutil.Uint64, 0, len(ret))
	for _, r := range ret {
		blockNum, ok, err := api.txnLookup(tx, r.Hash)
		if err != nil {
			return nil, err
		}
		if !ok {
			log.Warn("unexpected error, couldn't find tx", "hash", r.Hash)
		}
		blocks = append(blocks, hexutil.Uint64(blockNum))
	}

	blocksSummary, err := api.newBlocksSummaryFromResults(ctx, tx, blocks)
	if err != nil {
		return nil, err
	}
	return &TransactionListResult{
		BlocksSummary: blocksSummary,
		Results:       ret,
	}, nil
}

// copied from eth_receipts.go
func (api *Otterscan2APIImpl) _getTransactionReceipt(ctx context.Context, tx kv.Tx, txnHash common.Hash) (map[string]interface{}, error) {
	var blockNum uint64
	var ok bool

	blockNum, ok, err := api.txnLookup(tx, txnHash)
	if err != nil {
		return nil, err
	}

	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	if !ok && cc.Bor == nil {
		return nil, nil
	}

	// if not ok and cc.Bor != nil then we might have a bor transaction.
	// Note that Private API returns 0 if transaction is not found.
	if !ok || blockNum == 0 {
		blockNumPtr, err := rawdb.ReadBorTxLookupEntry(tx, txnHash)
		if err != nil {
			return nil, err
		}
		if blockNumPtr == nil {
			return nil, nil
		}

		blockNum = *blockNumPtr
	}

	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	var txnIndex uint64
	var txn types.Transaction
	for idx, transaction := range block.Transactions() {
		if transaction.Hash() == txnHash {
			txn = transaction
			txnIndex = uint64(idx)
			break
		}
	}

	var borTx types.Transaction
	// if txn == nil {
	// 	borTx = rawdb.ReadBorTransactionForBlock(tx, block)
	// 	if borTx == nil {
	// 		return nil, nil
	// 	}
	// }

	receipts, err := api.getReceipts(ctx, tx, cc, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}

	if txn == nil {
		borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), blockNum, receipts)
		if err != nil {
			return nil, err
		}
		if borReceipt == nil {
			return nil, nil
		}
		return marshalReceipt(borReceipt, borTx, cc, block.HeaderNoCopy(), txnHash, false), nil
	}

	if len(receipts) <= int(txnIndex) {
		return nil, fmt.Errorf("block has less receipts than expected: %d <= %d, block: %d", len(receipts), int(txnIndex), blockNum)
	}

	return marshalReceipt(receipts[txnIndex], block.Transactions()[txnIndex], cc, block.HeaderNoCopy(), txnHash, true), nil
}
