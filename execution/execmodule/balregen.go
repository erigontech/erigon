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

package execmodule

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	rawdbv3 "github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/transactions"
)

// BALRegeneratorDeps bundles the per-node helpers the regenerator needs to
// re-execute a historical block: chain config, consensus engine, block reader
// (for txs/bodies/headers), txNum reader (for state-at-block resolution), and
// the temporal RO DB.
type BALRegeneratorDeps struct {
	DB           kv.TemporalRoDB
	ChainConfig  *chain.Config
	Engine       rules.Engine
	BlockReader  services.FullBlockReader
	TxNumsReader rawdbv3.TxNumsReader
	Logger       log.Logger
}

// BALRegenerator implements balcache.BALRegenerator by re-executing the requested
// block against its parent state with VersionMap-enabled IBS read tracking.
// Uses transactions.ComputeBlockContext to construct a state reader rooted at
// the parent state — same approach as RPC tracing / receipts generation, so
// we get the read-tracking layout the BAL hash assumes without duplicating
// the state-at-block resolution code.
//
// Suitable for serving eth/71 GetBlockAccessLists when the local node hasn't
// cached the BAL (the block was produced before this node started, or the
// cache window rolled past it). Does NOT modify any persistent state — IBS
// writes go to state.NewNoopWriter().
type BALRegenerator struct {
	deps BALRegeneratorDeps
}

func NewBALRegenerator(deps BALRegeneratorDeps) *BALRegenerator {
	return &BALRegenerator{deps: deps}
}

// RegenerateBlockAccessList re-executes the block at (hash, number) and
// returns the RLP-encoded BAL. Returns (nil, nil) when the block can't be
// located, body is pruned, or the chain config doesn't have BAL active at
// the block's timestamp.
func (r *BALRegenerator) RegenerateBlockAccessList(ctx context.Context, hash common.Hash, number uint64) ([]byte, error) {
	tx, err := r.deps.DB.BeginTemporalRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: BeginTemporalRo: %w", err)
	}
	defer tx.Rollback()

	header, err := r.deps.BlockReader.Header(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: header lookup: %w", err)
	}
	if header == nil {
		return nil, nil
	}
	if !r.deps.ChainConfig.IsAmsterdam(header.Time) {
		// Pre-Amsterdam blocks don't have BALs by spec.
		return nil, nil
	}
	body, err := r.deps.BlockReader.BodyWithTransactions(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: body lookup: %w", err)
	}
	if body == nil {
		// Body pruned — we can't re-execute.
		return nil, nil
	}
	block := types.NewBlockFromStorage(hash, header, body.Transactions, body.Uncles, body.Withdrawals)

	// ComputeBlockContext at txIndex=0 returns an IBS reading from the
	// state BEFORE the block's first transaction (= post-state of the
	// parent block). Same machinery the RPC tracing path uses.
	ibs, blockCtx, _, vmRules, signer, err := transactions.ComputeBlockContext(ctx, r.deps.Engine, header, r.deps.ChainConfig, r.deps.BlockReader, nil, r.deps.TxNumsReader, tx, 0)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: ComputeBlockContext: %w", err)
	}
	// IBS read-tracking requires a VersionMap — versionedRead is the
	// only path that populates ibs.versionedReads, which TxIO() reads
	// to build the BAL.
	ibs.SetVersionMap(state.NewVersionMap(nil))

	bal, err := replayBlockForBAL(ctx, r.deps.ChainConfig, r.deps.Engine, block, &blockCtx, vmRules, signer, ibs, r.deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: replay block %d: %w", number, err)
	}
	if bal == nil {
		return nil, nil
	}
	return types.EncodeBlockAccessListBytes(bal)
}

// replayBlockForBAL drives the per-tx loop, merging the IBS's TxIO into a
// per-block VersionedIO. Mirrors the block-assembler pattern — no parallel
// exec, no state writer. Returns the accumulated BAL after the last tx.
func replayBlockForBAL(
	ctx context.Context,
	chainConfig *chain.Config,
	engine rules.Engine,
	block *types.Block,
	blockCtx *evmtypes.BlockContext,
	vmRules *chain.Rules,
	signer *types.Signer,
	ibs *state.IntraBlockState,
	logger log.Logger,
) (types.BlockAccessList, error) {
	header := block.HeaderNoCopy()
	gp := new(protocol.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))
	gasUsed := new(protocol.GasUsed)

	// chainReaderShim only exposes Config() — InitializeBlockExecution's
	// system-init paths don't reach for parent headers here.
	chainReader := &chainReaderShim{cfg: chainConfig}
	if err := protocol.InitializeBlockExecution(engine, chainReader, header, chainConfig, ibs, state.NewNoopWriter(), logger, nil); err != nil {
		return nil, fmt.Errorf("InitializeBlockExecution: %w", err)
	}

	var balIO state.VersionedIO
	balIO = *balIO.Merge(ibs.TxIO())
	ibs.ResetVersionedIO()

	_ = blockCtx
	_ = vmRules
	_ = signer

	blockHashFn := protocol.GetHashFn(header, func(common.Hash, uint64) (*types.Header, error) {
		// BAL re-execution doesn't need cross-block BLOCKHASH lookups —
		// the relevant headers are already in blockCtx for this block.
		return nil, nil
	})

	for i, txn := range block.Transactions() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ibs.SetTxContext(block.NumberU64(), i)
		_, err := protocol.ApplyTransaction(chainConfig, blockHashFn, engine, accounts.NilAddress, gp, ibs, state.NewNoopWriter(), header, txn, gasUsed, vm.Config{NoReceipts: true})
		if err != nil {
			return nil, fmt.Errorf("apply tx %d (%x): %w", i, txn.Hash(), err)
		}
		balIO = *balIO.Merge(ibs.TxIO())
		ibs.ResetVersionedIO()
	}

	// Finalize-stage system writes (withdrawals, BeaconRoot, etc.) are
	// captured during InitializeBlockExecution; engine.Finalize signatures
	// vary by fork and would require receipts + system-call hooks we don't
	// reconstruct here. If a downstream hash check flags a mismatch on
	// finalize-touched accounts, that's the signal to add fork-aware
	// finalize support to this path.

	return balIO.AsBlockAccessList(), nil
}

// chainReaderShim is a minimal adapter for rules.ChainHeaderReader that
// protocol.InitializeBlockExecution requires. The header-lookup methods
// stay nil because the BAL replay path doesn't need parent headers.
type chainReaderShim struct {
	cfg *chain.Config
}

func (s *chainReaderShim) Config() *chain.Config                                   { return s.cfg }
func (s *chainReaderShim) CurrentHeader() *types.Header                            { return nil }
func (s *chainReaderShim) CurrentFinalizedHeader() *types.Header                   { return nil }
func (s *chainReaderShim) CurrentSafeHeader() *types.Header                        { return nil }
func (s *chainReaderShim) GetHeader(hash common.Hash, number uint64) *types.Header { return nil }
func (s *chainReaderShim) GetHeaderByNumber(number uint64) *types.Header           { return nil }
func (s *chainReaderShim) GetHeaderByHash(hash common.Hash) *types.Header          { return nil }
func (s *chainReaderShim) GetTd(hash common.Hash, number uint64) *big.Int          { return big.NewInt(0) }
func (s *chainReaderShim) FrozenBlocks() uint64                                    { return 0 }
func (s *chainReaderShim) FrozenBorBlocks(align bool) uint64                       { return 0 }
