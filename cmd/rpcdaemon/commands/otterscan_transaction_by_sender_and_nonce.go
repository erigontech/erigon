package commands

import (
	"bytes"
	"context"
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func (api *OtterscanAPIImpl) GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (*common.Hash, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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
	var acc accounts.Account

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
	hash, err := rawdb.ReadCanonicalHash(tx, blockNum)
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
