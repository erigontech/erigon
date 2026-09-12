package blockreplay

import (
	"context"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// Capture re-executes block blockNum via a recordingReader and returns a
// self-contained Fixture: the pre-state the block reads, its RLP, the parent
// header, and the BLOCKHASH ancestor hashes.
func Capture(
	ctx context.Context,
	tx kv.TemporalTx,
	blockReader dbservices.FullBlockReader,
	chainConfig *chain.Config,
	engine rules.Engine,
	blockNum uint64,
	logger log.Logger,
) (*Fixture, error) {
	hash, ok, err := blockReader.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("canonical hash %d: %w", blockNum, err)
	}
	if !ok {
		return nil, fmt.Errorf("no canonical hash for block %d", blockNum)
	}
	block, _, err := blockReader.BlockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read block %d: %w", blockNum, err)
	}
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	parent, err := blockReader.Header(ctx, tx, block.ParentHash(), blockNum-1)
	if err != nil {
		return nil, fmt.Errorf("read parent header %d: %w", blockNum-1, err)
	}
	if parent == nil {
		return nil, fmt.Errorf("parent header %d not found", blockNum-1)
	}

	txNums := blockReader.TxnumReader()
	blockStartTxNum, err := txNums.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("min txNum %d: %w", blockNum, err)
	}

	rec := NewRecordingReader(state.NewHistoryReaderV3(tx, blockStartTxNum))
	rw := newRecordingWriter()
	chainReader := consensuschain.NewReader(chainConfig, tx, blockReader, logger)

	getHeader := func(h common.Hash, n uint64) (*types.Header, error) {
		return blockReader.Header(ctx, tx, h, n)
	}
	blockHashFunc := protocol.GetHashFn(block.Header(), getHeader)

	vmConfig := vm.Config{}
	if _, err := protocol.ExecuteBlockEphemerally(
		chainConfig, &vmConfig, blockHashFunc,
		engine, block,
		rec, rw,
		chainReader, nil, logger,
	); err != nil {
		return nil, fmt.Errorf("capture exec block %d: %w", blockNum, err)
	}

	fx := rec.Fixture()
	// The block's exec tells us which cells it wrote (rw.out keys); the reference
	// values come from canonical committed history, not the executor's own
	// computed values (that would make the oracle the code under test).
	postTxNum, err := txNums.Max(ctx, tx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("max txNum %d: %w", blockNum, err)
	}
	fx.Outputs, err = CollectOutputs(state.NewHistoryReaderV3(tx, postTxNum+1), rw.out)
	if err != nil {
		return nil, fmt.Errorf("capture outputs %d: %w", blockNum, err)
	}
	for _, s := range block.Body().SendersFromTxs() {
		fx.Senders = append(fx.Senders, [20]byte(s))
	}
	if fx.BlockRLP, err = rlpEncode(block); err != nil {
		return nil, err
	}
	if fx.ParentHeaderRLP, err = rlpEncodeHeader(parent); err != nil {
		return nil, err
	}
	captureAncestors(ctx, tx, blockReader, block.Header(), fx)
	return fx, nil
}

// MergeRangeOutputs computes the range-final post-state: the union of every key
// written by any block in the range, read at lastBlock's post-txNum from
// canonical history.
func MergeRangeOutputs(ctx context.Context, tx kv.TemporalTx, blockReader dbservices.FullBlockReader, blocks []*Fixture, lastBlock uint64) (*Outputs, error) {
	want := newOutputs()
	for _, b := range blocks {
		if b.Outputs == nil {
			continue
		}
		for a := range b.Outputs.Accounts {
			want.Accounts[a] = acctData{}
		}
		for a := range b.Outputs.Deleted {
			want.Accounts[a] = acctData{}
		}
		for a, slots := range b.Outputs.Storage {
			m := want.Storage[a]
			if m == nil {
				m = map[[32]byte][32]byte{}
				want.Storage[a] = m
			}
			for k := range slots {
				m[k] = [32]byte{}
			}
		}
		for a := range b.Outputs.Code {
			want.Code[a] = nil
		}
	}
	postTxNum, err := blockReader.TxnumReader().Max(ctx, tx, lastBlock)
	if err != nil {
		return nil, fmt.Errorf("max txNum %d: %w", lastBlock, err)
	}
	return CollectOutputs(state.NewHistoryReaderV3(tx, postTxNum+1), want)
}

// captureAncestors records the last 256 ancestor hashes so replay can answer
// the BLOCKHASH opcode without a DB.
func captureAncestors(ctx context.Context, tx kv.TemporalTx, blockReader dbservices.FullBlockReader, header *types.Header, fx *Fixture) {
	n := header.Number.Uint64()
	lo := uint64(0)
	if n > 256 {
		lo = n - 256
	}
	for a := lo; a < n; a++ {
		h, ok, err := blockReader.CanonicalHash(ctx, tx, a)
		if err != nil || !ok {
			continue
		}
		fx.Ancestors[a] = h
	}
}

// Replay re-executes the fixture's block against an in-memory reader with no DB.
// It validates receipts/gas/bloom but computes no commitment. readNanos>0 models
// a per-storage-read latency to reproduce a read-bound block's shape.
func Replay(
	fx *Fixture,
	chainConfig *chain.Config,
	engine rules.Engine,
	readNanos int64,
	logger log.Logger,
) (*protocol.EphemeralExecResult, error) {
	block, err := rlpDecodeBlock(fx.BlockRLP)
	if err != nil {
		return nil, err
	}
	if len(fx.Senders) > 0 {
		senders := make([]common.Address, len(fx.Senders))
		for i, s := range fx.Senders {
			senders[i] = common.Address(s)
		}
		block.SendersToTxs(senders)
	}

	reader := NewInMemReader(fx)
	reader.StorageReadNanos = readNanos

	blockHashFunc := func(n uint64) (common.Hash, error) {
		if h, ok := fx.Ancestors[n]; ok {
			return common.Hash(h), nil
		}
		return common.Hash{}, nil
	}

	cr, err := newFixtureChainReader(chainConfig, fx)
	if err != nil {
		return nil, err
	}

	vmConfig := vm.Config{}
	return protocol.ExecuteBlockEphemerally(
		chainConfig, &vmConfig, blockHashFunc,
		engine, block,
		reader, state.NewNoopWriter(),
		cr, nil, logger,
	)
}

// fixtureChainReader is a DB-free rules.ChainReader backed by the fixture: it
// serves only the parent header, which is all block execution consults beyond
// state and BLOCKHASH.
type fixtureChainReader struct {
	config *chain.Config
	parent *types.Header
}

func newFixtureChainReader(config *chain.Config, fx *Fixture) (*fixtureChainReader, error) {
	parent, err := rlpDecodeHeader(fx.ParentHeaderRLP)
	if err != nil {
		return nil, err
	}
	return &fixtureChainReader{config: config, parent: parent}, nil
}

func (c *fixtureChainReader) Config() *chain.Config                     { return c.config }
func (c *fixtureChainReader) CurrentHeader() *types.Header              { return c.parent }
func (c *fixtureChainReader) CurrentFinalizedHeader() *types.Header     { return c.parent }
func (c *fixtureChainReader) CurrentSafeHeader() *types.Header          { return c.parent }
func (c *fixtureChainReader) GetHeaderByNumber(uint64) *types.Header    { return c.parent }
func (c *fixtureChainReader) GetHeaderByHash(common.Hash) *types.Header { return c.parent }
func (c *fixtureChainReader) FrozenBlocks() uint64                      { return 0 }
func (c *fixtureChainReader) FrozenBorBlocks(bool) uint64               { return 0 }

func (c *fixtureChainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if c.parent != nil && c.parent.Number.Uint64() == number {
		return c.parent
	}
	return nil
}

func (c *fixtureChainReader) GetTd(common.Hash, uint64) *uint256.Int    { return uint256.NewInt(0) }
func (c *fixtureChainReader) GetBlock(common.Hash, uint64) *types.Block { return nil }
func (c *fixtureChainReader) HasBlock(common.Hash, uint64) bool         { return false }

var _ rules.ChainReader = (*fixtureChainReader)(nil)
