package commands

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type ContractCreatorData struct {
	Tx      common.Hash    `json:"hash"`
	Creator common.Address `json:"creator"`
}

func (api *OtterscanAPIImpl) GetContractCreator(ctx context.Context, addr common.Address) (*ContractCreatorData, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	reader := state.NewPlainStateReader(tx)
	plainStateAcc, err := reader.ReadAccountData(addr)
	if err != nil {
		return nil, err
	}

	// No state == non existent
	if plainStateAcc == nil {
		return nil, nil
	}

	// EOA?
	if plainStateAcc.IsEmptyCodeHash() {
		return nil, nil
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	var acc accounts.Account
	if api.historyV3(tx) {
		ttx := tx.(kv.TemporalTx)
		headNumber, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return nil, err
		}
		lastTxNum, err := rawdbv3.TxNums.Max(tx, headNumber)
		if err != nil {
			return nil, err
		}

		// Contract; search for creation tx; navigate forward on AccountsHistory/ChangeSets
		//
		// We traversing history Index - because it's cheaper than traversing History
		// and probe History periodically. In result will have small range of blocks. For binary search or full-scan.
		//
		// popular contracts may have dozens of states changes due to ETH deposits/withdraw after contract creation,
		// so it is optimal to search from the beginning even if the contract has multiple
		// incarnations.
		var prevTxnID, nextTxnID uint64
		it, err := ttx.IndexRange(temporal.AccountsHistoryIdx, addr[:], 0, int(lastTxNum+1), order.Asc, kv.Unlim)
		if err != nil {
			return nil, err
		}
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
			fmt.Printf("i: %d, %t, %x\n", i, ok, v)

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
			// Found the shard where the incarnation change happens; ignore all next index values
			if acc.Incarnation >= plainStateAcc.Incarnation {
				nextTxnID = txnID
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
			if acc.Incarnation < plainStateAcc.Incarnation {
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
		txIndex := int(creationTxnID) - int(minTxNum) - 1 /* system-contract */
		if txIndex == -1 {
			txIndex = (idx + int(prevTxnID)) - int(minTxNum) - 1
		}

		// Trace block, find tx and contract creator
		tracer := NewCreateTracer(ctx, addr)
		if err := api.genericTracer(tx, ctx, bn, creationTxnID, txIndex, chainConfig, tracer); err != nil {
			return nil, err
		}
		return &ContractCreatorData{
			Tx:      tracer.Tx.Hash(),
			Creator: tracer.Creator,
		}, nil
	}

	// Contract; search for creation tx; navigate forward on AccountsHistory/ChangeSets
	//
	// We search shards in forward order on purpose because popular contracts may have
	// dozens of states changes due to ETH deposits/withdraw after contract creation,
	// so it is optimal to search from the beginning even if the contract has multiple
	// incarnations.
	accHistory, err := tx.Cursor(kv.AccountsHistory)
	if err != nil {
		return nil, err
	}
	defer accHistory.Close()

	accCS, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return nil, err
	}
	defer accCS.Close()

	// Locate shard that contains the block where incarnation changed
	acs := historyv2.Mapper[kv.AccountChangeSet]
	k, v, err := accHistory.Seek(acs.IndexChunkKey(addr.Bytes(), 0))
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(k, addr.Bytes()) {
		log.Error("Couldn't find any shard for account history", "addr", addr)
		return nil, fmt.Errorf("could't find any shard for account history addr=%v", addr)
	}

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)
	prevShardMaxBl := uint64(0)
	for {
		_, err := bm.ReadFrom(bytes.NewReader(v))
		if err != nil {
			return nil, err
		}

		// Shortcut precheck
		st, err := acs.Find(accCS, bm.Maximum(), addr.Bytes())
		if err != nil {
			return nil, err
		}
		if st == nil {
			log.Error("Unexpected error, couldn't find changeset", "block", bm.Maximum(), "addr", addr)
			return nil, fmt.Errorf("unexpected error, couldn't find changeset block=%v addr=%v", bm.Maximum(), addr)
		}

		// Found the shard where the incarnation change happens; ignore all
		// next shards
		if err := acc.DecodeForStorage(st); err != nil {
			return nil, err
		}
		if acc.Incarnation >= plainStateAcc.Incarnation {
			break
		}
		prevShardMaxBl = bm.Maximum()

		k, v, err = accHistory.Next()
		if err != nil {
			return nil, err
		}

		// No more shards; it means the max bl from previous shard
		// contains the incarnation change
		if !bytes.HasPrefix(k, addr.Bytes()) {
			break
		}
	}

	// Binary search block number inside shard; get first block where desired
	// incarnation appears
	blocks := bm.ToArray()
	var searchErr error
	r := sort.Search(len(blocks), func(i int) bool {
		bl := blocks[i]
		st, err := acs.Find(accCS, bl, addr.Bytes())
		if err != nil {
			searchErr = err
			return false
		}
		if st == nil {
			log.Error("Unexpected error, couldn't find changeset", "block", bl, "addr", addr)
			return false
		}

		if err := acc.DecodeForStorage(st); err != nil {
			searchErr = err
			return false
		}
		if acc.Incarnation < plainStateAcc.Incarnation {
			return false
		}
		return true
	})

	if searchErr != nil {
		return nil, searchErr
	}

	// The sort.Search function finds the first block where the incarnation has
	// changed to the desired one, so we get the previous block from the bitmap;
	// however if the found block is already the first one from the bitmap, it means
	// the block we want is the max block from the previous shard.
	blockFound := prevShardMaxBl
	if r > 0 {
		blockFound = blocks[r-1]
	}
	// Trace block, find tx and contract creator
	tracer := NewCreateTracer(ctx, addr)
	if err := api.genericTracer(tx, ctx, blockFound, 0, 0, chainConfig, tracer); err != nil {
		return nil, err
	}

	return &ContractCreatorData{
		Tx:      tracer.Tx.Hash(),
		Creator: tracer.Creator,
	}, nil
}
