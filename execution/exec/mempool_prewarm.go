// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package exec

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	commitmentv3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const (
	mempoolPrewarmWorkers  = 4
	mempoolPrewarmInterval = 250 * time.Millisecond
	mempoolPrewarmMaxTxs   = 20_000
)

var errMempoolPrewarmHeadChanged = errors.New("mempool prewarm head changed")

type MempoolTransactions func(ctx context.Context, head common.Hash, height, gasBudget uint64, limit int, seen mapset.Set[[32]byte]) (types.Transactions, error)

type mempoolPrewarmStats struct {
	accounts, slots               int
	branchKeys                    []string
	branchKeysWalked, branchReads uint64
}

type mempoolPrewarmGetter struct {
	execctxapi.StateGetter
	ctx          context.Context
	stats        *mempoolPrewarmStats
	seenBranches mapset.Set[string]
}

func (g *mempoolPrewarmGetter) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if err := g.ctx.Err(); err != nil {
		return nil, 0, err
	}
	switch domain {
	case kv.AccountsDomain:
		g.stats.accounts++
	case kv.StorageDomain:
		g.stats.slots++
	default:
		return g.StateGetter.GetLatest(domain, key, opts)
	}
	value, step, err := g.StateGetter.GetLatest(domain, key, opts)
	if err == nil && g.seenBranches != nil {
		plainKey := string(key)
		if g.seenBranches.Add(plainKey) {
			g.stats.branchKeys = append(g.stats.branchKeys, plainKey)
		}
	}
	return value, step, err
}

func (bra *BlockReadAheader) PrewarmMempool(
	ctx context.Context, db kv.TemporalRoDB, reader dbservices.CanonicalReader,
	config *chain.Config, engine rules.EngineReader, head *types.Header, provide MempoolTransactions, logger log.Logger,
) {
	start := time.Now()
	headHash, height := head.Hash(), head.Number.Uint64()
	gasBudget := 2 * min(head.GasLimit, math.MaxUint64/2)
	ticker := time.NewTicker(mempoolPrewarmInterval)
	defer ticker.Stop()
	seen := mapset.NewSet[[32]byte]()
	var seenBranches mapset.Set[string]
	if execctx.PickTrieVariant() == commitment.VariantCommitmentV3 {
		seenBranches = mapset.NewSet[string]()
	}
	var stats mempoolPrewarmStats
	var rounds int
	var lastErr error
	for ctx.Err() == nil {
		remaining := mempoolPrewarmMaxTxs - seen.Cardinality()
		rounds++
		txns, err := provide(ctx, headHash, height, gasBudget, remaining, seen)
		if err == nil && ctx.Err() == nil && len(txns) != 0 {
			var roundStats mempoolPrewarmStats
			roundStats, err = bra.prewarmMempoolTxns(ctx, db, reader, config, engine, head, headHash, txns[:min(remaining, len(txns))], seen, seenBranches)
			stats.accounts += roundStats.accounts
			stats.slots += roundStats.slots
			stats.branchKeysWalked += roundStats.branchKeysWalked
			stats.branchReads += roundStats.branchReads
		}
		if err != nil {
			lastErr = err
		}
		if errors.Is(err, errMempoolPrewarmHeadChanged) || seen.Cardinality() == mempoolPrewarmMaxTxs {
			break
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	logger.Debug("[mempool prewarm] pass", "head", headHash, "rounds", rounds, "txs", seen.Cardinality(),
		"accounts", stats.accounts, "slots", stats.slots, "branchKeysWalked", stats.branchKeysWalked, "branchReads", stats.branchReads,
		"elapsed", time.Since(start), "cancelled", ctx.Err() != nil, "err", lastErr)
}

func checkMempoolPrewarmHead(tx kv.TemporalTx, hash common.Hash, height uint64) error {
	if rawdb.ReadHeadBlockHash(tx) != hash {
		return errMempoolPrewarmHeadChanged
	}
	progress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	if progress != height {
		return errMempoolPrewarmHeadChanged
	}
	return nil
}

func (bra *BlockReadAheader) prewarmMempoolTxns(
	ctx context.Context, db kv.TemporalRoDB, reader dbservices.CanonicalReader,
	config *chain.Config, engine rules.EngineReader, head *types.Header, headHash common.Hash, txns types.Transactions, seen mapset.Set[[32]byte], seenBranches mapset.Set[string],
) (mempoolPrewarmStats, error) {
	var next atomic.Int64
	var stats [mempoolPrewarmWorkers]mempoolPrewarmStats
	group, workerCtx := errgroup.WithContext(kv.WithNonBlockingAcquire(ctx))
	signer := types.MakeSigner(config, head.Number.Uint64(), head.Time)
	for worker := range min(mempoolPrewarmWorkers, len(txns)) {
		group.Go(func() error {
			tx, err := db.BeginTemporalRo(workerCtx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if err := checkMempoolPrewarmHead(tx, headHash, head.Number.Uint64()); err != nil {
				return err
			}
			getter := &mempoolPrewarmGetter{
				StateGetter: readAheadGetter(tx, bra.stateCache, true), ctx: workerCtx, stats: &stats[worker], seenBranches: seenBranches,
			}
			ibs := state.New(state.NewReaderV3(getter))
			defer ibs.Close()
			ibs.SetTxContext(head.Number.Uint64(), 0)
			blockHash := func(number uint64) (common.Hash, error) {
				hash, _, err := reader.CanonicalHash(workerCtx, tx, number)
				return hash, err
			}
			blockCtx := protocol.NewEVMBlockContext(head, blockHash, engine, accounts.InternAddress(head.Coinbase), config)
			evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, config, vm.Config{NoBAL: true})
			gp := protocol.NewGasPool(0, 0)

			for i := int(next.Add(1) - 1); i < len(txns); i = int(next.Add(1) - 1) {
				if err := workerCtx.Err(); err != nil {
					return err
				}
				txn := txns[i]
				msg, err := txn.AsMessage(*signer, head.BaseFee, evm.ChainRules())
				if err != nil || !seen.Add(txn.Hash()) {
					continue
				}
				msg.SetCheckNonce(false)
				ibs.Reset()
				ibs.SetNoConflictDetection()
				gp.Reset(head.GasLimit, txn.GetBlobGas())
				evm.Reset(protocol.NewEVMTxContext(msg), ibs)
				stop := context.AfterFunc(workerCtx, evm.Cancel)
				_, _ = protocol.ApplyMessage(evm, msg, gp, true, false, engine)
				if !stop() {
					return workerCtx.Err()
				}
				if err := ibs.StateReadError(); err != nil {
					return err
				}
			}
			if seenBranches != nil {
				return walkMempoolV3Branches(workerCtx, tx, &stats[worker])
			}
			return nil
		})
	}
	err := group.Wait()
	var total mempoolPrewarmStats
	for _, worker := range stats {
		total.accounts += worker.accounts
		total.slots += worker.slots
		total.branchKeysWalked += worker.branchKeysWalked
		total.branchReads += worker.branchReads
	}
	return total, err
}

func walkMempoolV3Branches(ctx context.Context, tx kv.TemporalTx, stats *mempoolPrewarmStats) error {
	var readErr error
	read := func(key []byte) []byte {
		if ctx.Err() != nil {
			return nil
		}
		stats.branchReads++
		data, _, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
		if err != nil {
			readErr = err
			return nil
		}
		return data
	}
	for _, plainKey := range stats.branchKeys {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := []byte(plainKey)
		account := commitmentv3.HashPrefetchKey(key[:length.Addr])
		var slotHash []byte
		if len(key) > length.Addr {
			slot := commitmentv3.HashPrefetchKey(key)
			slotHash = slot[:]
		}
		commitmentv3.PrefetchPath(read, account[:], slotHash)
		if readErr != nil {
			return readErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.branchKeysWalked++
	}
	return nil
}
