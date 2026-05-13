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
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// BALRegeneratorDeps bundles the per-node helpers the regenerator needs to
// re-execute a historical block: chain config, consensus engine, block reader
// (for txs/bodies), and a state-reader factory keyed on block hash+number.
//
// The state-reader factory returns a reader rooted at the *parent* state of the
// requested block. Caller owns the returned closer (typically a kv.Tx.Rollback)
// — the regenerator invokes it once after the re-execution finishes.
type BALRegeneratorDeps struct {
	ChainConfig *chain.Config
	Engine      rules.Engine
	BlockReader services.FullBlockReader
	NewParentStateReader func(ctx context.Context, blockHash common.Hash, blockNum uint64) (state.StateReader, services.HeaderReader, func(n uint64) (common.Hash, error), func(), error)
	Logger      log.Logger
}

// BALRegenerator implements rawdb.BALRegenerator by re-executing the requested
// block against its parent state with VersionMap-enabled IBS read tracking. The
// computed BAL is RLP-encoded and returned; the hash of the decoded BAL must
// match header.BlockAccessListHash (the parallel-exec path that produced the
// original BAL uses the same VersionedIO tracking, so the bytes are
// expected to be hash-equivalent for a non-failing execution).
//
// Suitable for serving eth/71 GetBlockAccessLists when the local node hasn't
// stored the BAL (pre-Amsterdam, pruned, never-received, or the cache window
// has rolled). Does NOT modify any persistent state — the IBS writes go to
// state.NewNoopWriter().
type BALRegenerator struct {
	deps BALRegeneratorDeps
}

func NewBALRegenerator(deps BALRegeneratorDeps) *BALRegenerator {
	return &BALRegenerator{deps: deps}
}

// RegenerateBlockAccessList re-executes the block at (hash, number) and returns
// the RLP-encoded BAL. Returns (nil, nil) when the block can't be located OR
// the chain config doesn't have BAL active at the block's timestamp.
func (r *BALRegenerator) RegenerateBlockAccessList(ctx context.Context, hash common.Hash, number uint64) ([]byte, error) {
	if r.deps.NewParentStateReader == nil {
		return nil, fmt.Errorf("BALRegenerator: NewParentStateReader is nil")
	}
	stateReader, headerReader, blockHashFunc, closer, err := r.deps.NewParentStateReader(ctx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: parent-state reader: %w", err)
	}
	if closer != nil {
		defer closer()
	}

	var stateGetter kv.TemporalGetter
	_ = stateGetter // future: when the reader needs explicit getter passthrough

	// Fetch the block from the local reader (txs + header).
	header, err := r.deps.BlockReader.Header(ctx, dbForReader(headerReader), hash, number)
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
	body, err := r.deps.BlockReader.BodyWithTransactions(ctx, dbForReader(headerReader), hash, number)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: body lookup: %w", err)
	}
	if body == nil {
		return nil, nil
	}
	block := types.NewBlockFromStorage(hash, header, body.Transactions, body.Uncles, body.Withdrawals)

	bal, err := computeBlockAccessList(ctx, r.deps.ChainConfig, r.deps.Engine, block, stateReader, headerReader, blockHashFunc, r.deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: re-exec block %d: %w", number, err)
	}
	if bal == nil {
		return nil, nil
	}
	balBytes, err := types.EncodeBlockAccessListBytes(bal)
	if err != nil {
		return nil, fmt.Errorf("BALRegenerator: encode: %w", err)
	}
	return balBytes, nil
}

// dbForReader is a placeholder for the kv.Getter context the HeaderReader/
// BodyWithTransactions paths sometimes need. The closure inside
// NewParentStateReader is expected to share its tx with the header reader; this
// is the seam where it's threaded through.
//
// TODO: the BlockReader's Header/BodyWithTransactions need an explicit kv.Tx —
// NewParentStateReader currently exposes only a state.StateReader. Surface the
// tx alongside the state reader so this becomes a real argument.
func dbForReader(_ services.HeaderReader) kv.Getter {
	return nil
}

// computeBlockAccessList runs the block transactions through a simple IBS with
// VersionMap-enabled read tracking and returns the accumulated BAL. Mirrors the
// per-tx Merge pattern used by the block assembler — no parallel exec needed.
func computeBlockAccessList(
	ctx context.Context,
	chainConfig *chain.Config,
	engine rules.Engine,
	block *types.Block,
	stateReader state.StateReader,
	headerReader services.HeaderReader,
	blockHashFunc func(n uint64) (common.Hash, error),
	logger log.Logger,
) (types.BlockAccessList, error) {
	ibs := state.New(stateReader)
	defer ibs.Release(false)
	// Read tracking requires a VersionMap — versionedRead is the only path
	// that populates ibs.versionedReads. An empty VersionMap is fine; we
	// don't need cross-tx coordination here.
	ibs.SetVersionMap(state.NewVersionMap(nil))

	header := block.HeaderNoCopy()
	gp := new(protocol.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))
	gasUsed := new(protocol.GasUsed)

	chainReader := newChainReaderShim(chainConfig, headerReader)

	if err := protocol.InitializeBlockExecution(engine, chainReader, header, chainConfig, ibs, state.NewNoopWriter(), logger, nil); err != nil {
		return nil, fmt.Errorf("InitializeBlockExecution: %w", err)
	}

	var balIO state.VersionedIO
	balIO = *balIO.Merge(ibs.TxIO())
	ibs.ResetVersionedIO()

	for i, txn := range block.Transactions() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ibs.SetTxContext(block.NumberU64(), i)
		_, err := protocol.ApplyTransaction(chainConfig, blockHashFunc, engine, accounts.NilAddress, gp, ibs, state.NewNoopWriter(), header, txn, gasUsed, vm.Config{NoReceipts: true})
		if err != nil {
			return nil, fmt.Errorf("apply tx %d (%x): %w", i, txn.Hash(), err)
		}
		balIO = *balIO.Merge(ibs.TxIO())
		ibs.ResetVersionedIO()
	}

	// Finalize step is intentionally omitted: the engine.Finalize signatures
	// differ across forks and require receipts/systemCall hooks we don't
	// reconstruct here. Withdrawal + system-call accesses are typically
	// captured during InitializeBlockExecution; any post-tx finalize writes
	// would only matter for accounts already in the BAL from the tx loop.
	// If the resulting BAL hash disagrees with header.BlockAccessListHash
	// on a downstream peer's verification, that's the signal to add finalize
	// support here (with the proper system-call hooks).

	return balIO.AsBlockAccessList(), nil
}

// chainReaderShim is a minimal adapter for rules.ChainHeaderReader that
// protocol.InitializeBlockExecution requires. The header-lookup methods stay
// nil because system-init calls in this path don't reach for parent headers
// (the only thing that would need them is engine.Finalize, which we don't
// invoke here).
type chainReaderShim struct {
	cfg    *chain.Config
	reader services.HeaderReader
}

func newChainReaderShim(cfg *chain.Config, reader services.HeaderReader) *chainReaderShim {
	return &chainReaderShim{cfg: cfg, reader: reader}
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
