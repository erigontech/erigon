package commands

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func (api *OtterscanAPIImpl) GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (*common.Hash, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var acc accounts.Account
	if api.historyV3(tx) {
		ttx := tx.(kv.TemporalTx)
		it, err := ttx.IndexRange(temporal.AccountsHistoryIdx, addr[:], -1, -1, order.Asc, kv.Unlim)
		if err != nil {
			return nil, err
		}

		var prevTxnID, nextTxnID uint64
		for i := 0; it.HasNext(); i++ {
			txnID, err := it.Next()
			if err != nil {
				return nil, err
			}

			if i%4096 != 0 { // probe history periodically, not on every change
				nextTxnID = txnID
				continue
			}

			v, ok, err := ttx.HistoryGet(temporal.AccountsHistory, addr[:], txnID)
			if err != nil {
				log.Error("Unexpected error, couldn't find changeset", "txNum", i, "addr", addr)
				return nil, err
			}
			if !ok {
				err = fmt.Errorf("couldn't find history txnID=%v addr=%v", txnID, addr)
				log.Error("[rpc] Unexpected error", "err", err)
				return nil, err
			}

			if len(v) == 0 { // creation, but maybe not our Incarnation
				prevTxnID = txnID
				continue
			}

			if err := acc.DecodeForStorage(v); err != nil {
				return nil, err
			}
			// Desired nonce was found in this chunk
			if acc.Nonce > nonce {
				break
			}
			prevTxnID = txnID
		}

		// The sort.Search function finds the first block where the incarnation has
		// changed to the desired one, so we get the previous block from the bitmap;
		// however if the creationTxnID block is already the first one from the bitmap, it means
		// the block we want is the max block from the previous shard.
		var creationTxnID uint64
		var searchErr error

		if nextTxnID == 0 {
			nextTxnID = prevTxnID + 1
		}
		// Binary search in [prevTxnID, nextTxnID] range; get first block where desired incarnation appears
		// can be replaced by full-scan over ttx.HistoryRange([prevTxnID, nextTxnID])?
		idx := sort.Search(int(nextTxnID-prevTxnID), func(i int) bool {
			txnID := uint64(i) + prevTxnID
			v, ok, err := ttx.HistoryGet(temporal.AccountsHistory, addr[:], txnID)
			if err != nil {
				log.Error("[rpc] Unexpected error, couldn't find changeset", "txNum", i, "addr", addr)
				panic(err)
			}
			if !ok {
				return false
			}
			if len(v) == 0 {
				creationTxnID = cmp.Max(creationTxnID, txnID)
				return false
			}

			if err := acc.DecodeForStorage(v); err != nil {
				searchErr = err
				return false
			}
			// Since the state contains the nonce BEFORE the block changes, we look for
			// the block when the nonce changed to be > the desired once, which means the
			// previous history block contains the actual change; it may contain multiple
			// nonce changes.
			if acc.Nonce <= nonce {
				creationTxnID = cmp.Max(creationTxnID, txnID)
				return false
			}
			return true
		})
		if searchErr != nil {
			return nil, searchErr
		}
		if creationTxnID == 0 {
			return nil, fmt.Errorf("binary search between %d-%d doesn't find anything", nextTxnID, prevTxnID)
		}
		ok, bn, err := rawdbv3.TxNums.FindBlockNum(tx, creationTxnID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("block not found by txnID=%d", creationTxnID)
		}
		minTxNum, err := rawdbv3.TxNums.Min(tx, bn)
		if err != nil {
			return nil, err
		}
		txIndex := int(creationTxnID) - int(minTxNum) - 1 /* system-tx */
		if txIndex == -1 {
			txIndex = (idx + int(prevTxnID)) - int(minTxNum) - 1
		}
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, ttx, bn, txIndex)
		if err != nil {
			return nil, err
		}
		if txn == nil {
			log.Warn("[rpc] tx is nil", "blockNum", bn, "txIndex", txIndex)
			return nil, nil
		}
		found := txn.GetNonce() == nonce
		if !found {
			return nil, nil
		}
		txHash := txn.Hash()
		return &txHash, nil
	}

	accHistoryC, err := tx.Cursor(kv.AccountsHistory)
	if err != nil {
		return nil, err
	}
	defer accHistoryC.Close()

	accChangesC, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return nil, err
	}
	defer accChangesC.Close()

	// Locate the chunk where the nonce happens
	acs := historyv2.Mapper[kv.AccountChangeSet]
	k, v, err := accHistoryC.Seek(acs.IndexChunkKey(addr.Bytes(), 0))
	if err != nil {
		return nil, err
	}

	bitmap := roaring64.New()
	maxBlPrevChunk := uint64(0)

	for {
		if k == nil || !bytes.HasPrefix(k, addr.Bytes()) {
			// Check plain state
			data, err := tx.GetOne(kv.PlainState, addr.Bytes())
			if err != nil {
				return nil, err
			}
			if err := acc.DecodeForStorage(data); err != nil {
				return nil, err
			}

			// Nonce changed in plain state, so it means the last block of last chunk
			// contains the actual nonce change
			if acc.Nonce > nonce {
				break
			}
			// Not found; asked for nonce still not used
			return nil, nil
		}

		// Inspect block changeset
		if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
			return nil, err
		}
		maxBl := bitmap.Maximum()
		data, err := acs.Find(accChangesC, maxBl, addr.Bytes())
		if err != nil {
			return nil, err
		}
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}

		// Desired nonce was found in this chunk
		if acc.Nonce > nonce {
			break
		}

		maxBlPrevChunk = maxBl
		k, v, err = accHistoryC.Next()
		if err != nil {
			return nil, err
		}
	}

	// Locate the exact block inside chunk when the nonce changed
	blocks := bitmap.ToArray()
	var errSearch error = nil
	idx := sort.Search(len(blocks), func(i int) bool {
		if errSearch != nil {
			return false
		}

		// Locate the block changeset
		data, err := acs.Find(accChangesC, blocks[i], addr.Bytes())
		if err != nil {
			errSearch = err
			return false
		}

		if err := acc.DecodeForStorage(data); err != nil {
			errSearch = err
			return false
		}

		// Since the state contains the nonce BEFORE the block changes, we look for
		// the block when the nonce changed to be > the desired once, which means the
		// previous history block contains the actual change; it may contain multiple
		// nonce changes.
		return acc.Nonce > nonce
	})
	if errSearch != nil {
		return nil, errSearch
	}

	// Since the changeset contains the state BEFORE the change, we inspect
	// the block before the one we found; if it is the first block inside the chunk,
	// we use the last block from prev chunk
	nonceBlock := maxBlPrevChunk
	if idx > 0 {
		nonceBlock = blocks[idx-1]
	}
	found, txHash, err := api.findNonce(ctx, tx, addr, nonce, nonceBlock)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	return &txHash, nil
}

func (api *OtterscanAPIImpl) findNonce(ctx context.Context, tx kv.Tx, addr common.Address, nonce uint64, blockNum uint64) (bool, common.Hash, error) {
	hash, err := api._blockReader.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return false, common.Hash{}, err
	}
	block, senders, err := api._blockReader.BlockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return false, common.Hash{}, err
	}

	txs := block.Transactions()
	for i, s := range senders {
		if s != addr {
			continue
		}

		t := txs[i]
		if t.GetNonce() == nonce {
			return true, t.Hash(), nil
		}
	}

	return false, common.Hash{}, nil
}
