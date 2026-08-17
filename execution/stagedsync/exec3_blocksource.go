package stagedsync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
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
	header(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error)
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
	canonicalHash, err := rawdb.ReadCanonicalHash(s.blockTx, blockNum)
	if err != nil {
		return nil, nil, err
	}
	b, ok := s.cfg.readAheader.ReadBlockWithSenders(canonicalHash)
	if b == nil || !ok {
		b, err = exec.BlockWithSenders(ctx, s.cfg.db, s.blockTx, s.cfg.blockReader, blockNum)
		if err != nil {
			return nil, nil, err
		}
	}
	if b == nil {
		return nil, nil, fmt.Errorf("nil block %d", blockNum)
	}

	var dbBAL types.BlockAccessList
	// Prefer the payload-carried BAL; fall back to the DB sidecar via blockTx
	// (overlay or execRoTx) — do NOT open a separate db.View() as it can deadlock
	// with the stageloop's RW transaction when BlockOverlay is active.
	data, err := blockAccessListBytes(s.blockTx, b, blockNum)
	if err != nil {
		return nil, nil, err
	}
	if len(data) > 0 && !dbg.IgnoreBAL {
		dbBAL, err = types.DecodeBlockAccessListBytes(data)
		if err != nil {
			return nil, nil, fmt.Errorf("decode block access list: %w", err)
		}
		if err := dbBAL.Validate(); err != nil {
			return nil, nil, fmt.Errorf("invalid block access list: %w", err)
		}
	}
	return b, dbBAL, nil
}

func (s *dbBlockSource) header(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error) {
	h, err := s.cfg.blockReader.Header(ctx, s.blockTx, hash, number)
	if h == nil && err == nil {
		h = &types.Header{}
	}
	return h, err
}
