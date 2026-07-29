package blockreplay

import (
	"context"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

// memBlockReader is a DB-free dbservices.FullBlockReader backed entirely by a
// captured fixture: the parallel executor's only block-data reads at replay time
// are BlockWithSenders (the block under validation), Header (BLOCKHASH), and
// FrozenBlocks. Everything else is a zero-value stub — nothing on the
// single-block replay path calls it.
type memBlockReader struct {
	block     *types.Block
	senders   []common.Address
	num       uint64
	parent    *types.Header
	parentN   uint64
	ancestors map[uint64]common.Hash // block number -> hash (BLOCKHASH range)
}

// NewMemBlockReader builds an in-memory block reader from the fixture's decoded
// block, parent header, and captured ancestor hashes.
func NewMemBlockReader(fx *Fixture) (dbservices.FullBlockReader, error) {
	block, err := fx.Block()
	if err != nil {
		return nil, err
	}
	if block.NumberU64() == 0 {
		return nil, fmt.Errorf("cannot replay genesis block (0): it has no parent")
	}
	parent, err := fx.ParentHeader()
	if err != nil {
		return nil, err
	}
	ancestors := make(map[uint64]common.Hash, len(fx.Ancestors))
	for n, h := range fx.Ancestors {
		ancestors[n] = common.Hash(h)
	}
	return &memBlockReader{
		block:     block,
		senders:   fx.SendersList(),
		num:       block.NumberU64(),
		parent:    parent,
		parentN:   block.NumberU64() - 1,
		ancestors: ancestors,
	}, nil
}

// headerAt serves headers the BLOCKHASH walk (GetHashFn) needs. It must NOT
// return nil for a number in the walk range: GetHashFn's caller turns a nil
// header into an empty one (Number 0), and Number-1 then underflows to
// MaxUint64, wedging the walk in an infinite loop. So for any ancestor at or
// below the parent it returns a synthetic header carrying the right Number and
// the captured parent-hash link, which both terminates the walk and yields the
// fixture's real ancestor hash.
func (r *memBlockReader) headerAt(number uint64) *types.Header {
	switch {
	case number == r.num:
		return r.block.HeaderNoCopy()
	case number == r.parentN:
		return r.parent
	case number < r.parentN:
		h := &types.Header{Number: *uint256.NewInt(number)}
		if prev, ok := r.ancestors[number-1]; ok {
			h.ParentHash = prev
		}
		return h
	default:
		return nil
	}
}

// --- HeaderReader ---

func (r *memBlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error) {
	return r.headerAt(blockNum), nil
}
func (r *memBlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error) {
	return r.headerAt(blockNum), nil
}
func (r *memBlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
	if r.block.Hash() == hash {
		return r.block.HeaderNoCopy(), nil
	}
	if r.parent.Hash() == hash {
		return r.parent, nil
	}
	return nil, nil
}
func (r *memBlockReader) HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	if r.block.Hash() == hash {
		n := r.num
		return &n, nil
	}
	if r.parent.Hash() == hash {
		n := r.parentN
		return &n, nil
	}
	return nil, nil
}
func (r *memBlockReader) ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	return common.Hash{}, 0
}
func (r *memBlockReader) HeadersRange(ctx context.Context, walker func(header *types.Header) error) error {
	return nil
}
func (r *memBlockReader) Integrity(ctx context.Context, tx kv.Getter) error { return nil }

// --- BlockReader ---

func (r *memBlockReader) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Block, []common.Address, error) {
	if blockNum == r.num {
		return r.block, r.senders, nil
	}
	return nil, nil, nil
}
func (r *memBlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	if number == r.num {
		return r.block, nil
	}
	return nil, nil
}
func (r *memBlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error) {
	if r.block.Hash() == hash {
		return r.block, nil
	}
	return nil, nil
}
func (r *memBlockReader) CurrentBlock(db kv.Tx) (*types.Block, error) { return r.block, nil }
func (r *memBlockReader) IterateFrozenBodies(tx kv.Getter, f func(blockNum, baseTxNum, txCount uint64) error) error {
	return nil
}
func (r *memBlockReader) MinimumBlockAvailable(ctx context.Context, tx kv.Tx) (uint64, error) {
	return 0, nil
}

// --- BodyReader ---

func (r *memBlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Body, error) {
	if blockNum == r.num {
		return r.block.Body(), nil
	}
	return nil, nil
}
func (r *memBlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (rlp.RawValue, error) {
	return nil, nil
}
func (r *memBlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Body, uint32, error) {
	if blockNum == r.num {
		b := r.block.Body()
		return b, uint32(len(b.Transactions)), nil
	}
	return nil, 0, nil
}
func (r *memBlockReader) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.BodyForStorage, error) {
	return nil, nil
}
func (r *memBlockReader) HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error) {
	return blockNum == r.num, nil
}
func (r *memBlockReader) BlockForTxNum(ctx context.Context, tx kv.Tx, txNum uint64) (uint64, bool, error) {
	return 0, false, nil
}

// --- TxnReader ---

func (r *memBlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, uint64, bool, error) {
	return 0, 0, false, nil
}
func (r *memBlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (types.Transaction, bool, error) {
	if blockNum == r.num && i >= 0 && i < len(r.block.Transactions()) {
		return r.block.Transactions()[i], true, nil
	}
	return nil, false, nil
}
func (r *memBlockReader) RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) ([][]byte, error) {
	return nil, nil
}
func (r *memBlockReader) FirstTxnNumNotInSnapshots(tx kv.Getter) uint64 { return 0 }

// --- CanonicalReader ---

func (r *memBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (common.Hash, bool, error) {
	switch blockNum {
	case r.num:
		return r.block.Hash(), true, nil
	case r.parentN:
		return r.parent.Hash(), true, nil
	}
	return common.Hash{}, false, nil
}
func (r *memBlockReader) IsCanonical(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error) {
	return true, nil
}
func (r *memBlockReader) BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	return nil, nil
}

// --- misc / freezing ---

func (r *memBlockReader) FrozenBlocks() uint64                      { return 0 }
func (r *memBlockReader) FrozenBorBlocks(align bool) uint64         { return 0 }
func (r *memBlockReader) FreezingCfg() ethconfig.BlocksFreezing     { return ethconfig.BlocksFreezing{} }
func (r *memBlockReader) CanPruneTo(currentBlockInDB uint64) uint64 { return 0 }
func (r *memBlockReader) Snapshots() dbservices.BlockSnapshots      { return nil }
func (r *memBlockReader) BorSnapshots() dbservices.BlockSnapshots   { return nil }
func (r *memBlockReader) AllTypes() []snaptype.Type                 { return nil }
func (r *memBlockReader) TxnumReader() rawdbv3.TxNumsReader         { return rawdbv3.TxNums }

func (r *memBlockReader) Ready(ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	ch <- nil
	close(ch)
	return ch
}

var _ dbservices.FullBlockReader = (*memBlockReader)(nil)
