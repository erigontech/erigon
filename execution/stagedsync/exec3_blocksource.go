package stagedsync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/types"
)

// blockSource drives execution: it yields blocks (and their access lists) in
// order via next, and resolves ancestor headers for the BLOCKHASH opcode. The
// range/sequence is the source's own business — the DB-backed implementation
// walks a [start,max] block range; an ephemeral implementation can serve a
// single captured block with no DB behind it.
type blockSource interface {
	// next returns the following block and its access list; more is false when
	// the source is exhausted.
	next(ctx context.Context) (b *types.Block, bal types.BlockAccessList, blockNum uint64, more bool, err error)
	header(ctx context.Context, hash common.Hash, number uint64) (*types.Header, bool, error)
}

// dbBlockSource reads blocks, their access lists, and ancestor headers through
// the stage's block reader / read-ahead cache over blockTx (an overlay or roTx),
// walking the [cur,max] block range.
type dbBlockSource struct {
	cfg     *ExecuteBlockCfg
	blockTx kv.Tx
	cur     uint64
	max     uint64
}

func (s *dbBlockSource) next(ctx context.Context) (*types.Block, types.BlockAccessList, uint64, bool, error) {
	if s.cur > s.max {
		return nil, nil, 0, false, nil
	}
	blockNum := s.cur
	s.cur++
	b, bal, err := s.blockAndBAL(ctx, blockNum)
	if err != nil {
		return nil, nil, blockNum, false, err
	}
	return b, bal, blockNum, true, nil
}

func (s *dbBlockSource) blockAndBAL(ctx context.Context, blockNum uint64) (*types.Block, types.BlockAccessList, error) {
	canonicalHash, ok, err := rawdb.ReadCanonicalHash(s.blockTx, blockNum)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("canonical hash not found: %d", blockNum)
	}
	b, ok := s.cfg.readAheader.ReadBlockWithSenders(canonicalHash)
	if b == nil || !ok {
		b, ok, err = exec.BlockWithSenders(ctx, s.cfg.db, s.blockTx, s.cfg.blockReader, blockNum)
		if err != nil {
			return nil, nil, err
		}
	}
	if !ok {
		return nil, nil, fmt.Errorf("block not found: %d", blockNum)
	}

	blockBAL, err := blockAccessList(s.blockTx, b, blockNum)
	if err != nil {
		return nil, nil, err
	}
	if b.BlockAccessList() == nil && blockBAL != nil {
		b = b.WithBlockAccessListSidecar(types.NewBlockAccessListSidecar(blockBAL))
	}
	return b, blockBAL, nil
}

func (s *dbBlockSource) header(ctx context.Context, hash common.Hash, number uint64) (*types.Header, bool, error) {
	return s.cfg.blockReader.Header(ctx, s.blockTx, hash, number)
}
