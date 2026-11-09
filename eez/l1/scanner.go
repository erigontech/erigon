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
package l1

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

var (
	// ErrSourceIncomplete reports a response the source assembled from
	// different forks of one block; a retry can succeed.
	ErrSourceIncomplete = errors.New("L1 source answered from mixed forks")
	// ErrUndecodable reports a post listing the rollup whose transaction is
	// not the batch its BatchPosted reports.
	ErrUndecodable = errors.New("posting transaction is not the batch BatchPosted reports")
	// ErrUnsupported reports a post listing the rollup that the follower
	// cannot verify: one made through a contract, one binding another L1
	// block than its own, or one whose public input binds custom data the
	// follower cannot reproduce.
	ErrUnsupported = errors.New("batch post the follower cannot verify")
)

// LogChunk is the widest range the scanner asks for at once.
const LogChunk = 100_000

// ScannedBatch is a posted batch and what L1 applied of it for the rollup.
type ScannedBatch struct {
	L1Block     uint64
	L1BlockHash common.Hash
	// L1Time and L1MixDigest are the timestamp and mixHash of the L1 block.
	L1Time      uint64
	L1MixDigest common.Hash
	TxHash      common.Hash
	TxIndex     uint64
	// Input is the posting transaction's input.
	Input []byte
	// BlobHashes are the versioned hashes of the posting transaction's
	// blobs.
	BlobHashes []common.Hash
	// Batch is the posted batch; its CallData, or the blobs BlobIndices
	// selects, carry the DA payload.
	Batch      *protocol.ProofSystemBatch
	Settlement protocol.Settlement
}

// Scanner reads the batches a registry settled for one rollup.
type Scanner struct {
	reader   ChainReader
	registry common.Address
	rollupID uint64
	chunk    uint64
	rollupX  bool
}

// NewScanner returns a scanner of the registry's settlements of rollupID.
func NewScanner(reader ChainReader, registry common.Address, rollupID uint64) *Scanner {
	return &Scanner{reader: reader, registry: registry, rollupID: rollupID, chunk: LogChunk}
}

// SetChunk sets the widest range the scanner asks for at once.
func (s *Scanner) SetChunk(n uint64) { s.chunk = max(n, 1) }

// UseRollupX makes the scanner attribute settlements as rollupX does: a
// batch is the rollup's when its BatchPosted lists it, and it settled the
// steps of its claimed root chain that L1 emitted.
func (s *Scanner) UseRollupX() { s.rollupX = true }

// RegistryTopics selects the registry logs a follower reads.
var RegistryTopics = [][]common.Hash{{
	protocol.BatchPostedTopic,
	protocol.L2ExecutionPerformedTopic,
	protocol.ExecutionConsumedTopic,
	protocol.L2TxSkippedTopic,
}}

// Scan returns the batches of [from, to] that settled something for the
// rollup, in L1 order, and the last block it read. That block is below to
// when the source refused the range and the scanner narrowed it; the caller
// continues from the block after it. Later calls keep the narrowed width.
func (s *Scanner) Scan(ctx context.Context, from, to uint64) ([]ScannedBatch, uint64, error) {
	hi := min(to, from+s.chunk-1)
	for {
		batches, err := s.scanRange(ctx, from, hi)
		if errors.Is(err, ErrRangeTooWide) && hi > from {
			hi = from + (hi-from)/2
			s.chunk = hi - from + 1
			continue
		}
		return batches, hi, err
	}
}

type blockKey struct {
	number uint64
	hash   common.Hash
}

func (s *Scanner) scanRange(ctx context.Context, from, to uint64) ([]ScannedBatch, error) {
	logs, err := s.reader.Logs(ctx, from, to, s.registry, RegistryTopics)
	if err != nil {
		return nil, err
	}
	// One query per range: separate queries can answer from different forks.
	forks := map[uint64]common.Hash{}
	byBlock := map[blockKey][]*types.Log{}
	var blocks []blockKey
	for _, l := range logs {
		n := uint64(l.BlockNumber)
		if h, ok := forks[n]; ok && h != l.BlockHash {
			return nil, fmt.Errorf("%w: block %d as %x and %x", ErrSourceIncomplete, n, h, l.BlockHash)
		}
		forks[n] = l.BlockHash
		k := blockKey{n, l.BlockHash}
		if byBlock[k] == nil {
			blocks = append(blocks, k)
		}
		byBlock[k] = append(byBlock[k], l)
	}
	slices.SortFunc(blocks, func(a, b blockKey) int { return cmp.Compare(a.number, b.number) })

	var out []ScannedBatch
	for _, k := range blocks {
		scanned, err := s.scanBlock(ctx, k, byBlock[k])
		if err != nil {
			return nil, err
		}
		out = append(out, scanned...)
	}
	return out, nil
}

func (s *Scanner) scanBlock(ctx context.Context, k blockKey, logs []*types.Log) ([]ScannedBatch, error) {
	slices.SortFunc(logs, func(a, b *types.Log) int { return cmp.Compare(a.Index, b.Index) })
	var read *Header
	header := func() (*Header, error) {
		if read != nil {
			return read, nil
		}
		h, err := s.reader.HeaderByHash(ctx, k.hash)
		if err != nil {
			return nil, err
		}
		if h.Number != k.number || h.Hash != k.hash {
			return nil, fmt.Errorf("%w: header %d (%x) served for the logs of block %d (%x)", ErrSourceIncomplete, h.Number, h.Hash, k.number, k.hash)
		}
		read = h
		return h, nil
	}

	var posted []protocol.PostedBatch
	var txHashes []common.Hash
	var inputs [][]byte
	var blobHashes [][]common.Hash
	for _, l := range logs {
		if l.Topics[0] != protocol.BatchPostedTopic {
			continue
		}
		// The registry lists the rollups whose proofs it verified, so only
		// BatchPosted can say a post is ours; a third party controls its
		// transaction.
		e, err := protocol.ParseBatchPosted(l)
		if err != nil {
			return nil, fmt.Errorf("%w: BatchPosted of transaction %x in block %d: %w", ErrUndecodable, l.TxHash, k.number, err)
		}
		if !slices.Contains(e.RollupIDs, s.rollupID) {
			continue
		}
		tx, err := s.reader.TransactionByBlockHashAndIndex(ctx, k.hash, uint64(l.TxIndex))
		if err != nil {
			return nil, err
		}
		if tx.Hash() != l.TxHash {
			return nil, fmt.Errorf("%w: transaction %d of block %d hashes to %x, its log names %x", ErrInconsistent, l.TxIndex, k.number, tx.Hash(), l.TxHash)
		}
		if to := tx.GetTo(); !s.rollupX && (to == nil || *to != s.registry) {
			return nil, fmt.Errorf("%w: transaction %x of block %d posts through a contract", ErrUnsupported, l.TxHash, k.number)
		}
		batch, err := protocol.DecodePostAndVerifyBatch(tx.GetData())
		if err != nil {
			return nil, fmt.Errorf("%w: transaction %x of block %d: %w", ErrUndecodable, l.TxHash, k.number, err)
		}
		if !slices.EqualFunc(batch.RollupIDsWithProofSystems, e.RollupIDs, func(r protocol.RollupIDWithProofSystems, id uint64) bool { return r.RollupID == id }) {
			return nil, fmt.Errorf("%w: transaction %x of block %d verifies other rollups than its BatchPosted lists", ErrUndecodable, l.TxHash, k.number)
		}
		if !s.rollupX {
			switch batch.BlockNumber {
			case 0:
			case protocol.LiveBlockNumber:
				h, err := header()
				if err != nil {
					return nil, err
				}
				if err := checkLiveContext(h, l, e, tx, batch); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("%w: transaction %x of block %d binds L1 block %d, neither timeless nor live", ErrUnsupported, l.TxHash, k.number, batch.BlockNumber)
			}
		}
		posted = append(posted, protocol.PostedBatch{Batch: batch, TxIndex: uint64(l.TxIndex), LogIndex: uint64(l.Index), ListsRollup: true})
		txHashes = append(txHashes, l.TxHash)
		inputs = append(inputs, tx.GetData())
		blobHashes = append(blobHashes, tx.GetBlobHashes())
	}

	attribute := protocol.AttributeBlock
	if s.rollupX {
		attribute = protocol.AttributeRollupXBlock
	}
	settlements, err := attribute(posted, logs, s.rollupID)
	if err != nil {
		return nil, fmt.Errorf("block %d (%x): %w", k.number, k.hash, err)
	}
	var out []ScannedBatch
	for i, p := range posted {
		if len(settlements[i].Applied) == 0 {
			continue
		}
		h, err := header()
		if err != nil {
			return nil, err
		}
		out = append(out, ScannedBatch{
			L1Block:     k.number,
			L1BlockHash: k.hash,
			L1Time:      h.Time,
			L1MixDigest: h.MixDigest,
			TxHash:      txHashes[i],
			TxIndex:     p.TxIndex,
			Input:       inputs[i],
			BlobHashes:  blobHashes[i],
			Batch:       p.Batch,
			Settlement:  settlements[i],
		})
	}
	return out, nil
}

// checkLiveContext requires BatchPosted e (log l) of the live batch b, which
// tx posted directly, to report the sharedPublicInput that binds the
// timestamp and parent of h, the L1 block including it, for every rollup b
// lists. The logs and h come from one block hash, so a mismatch is
// permanent: with one rollup the post contradicts itself; with several,
// another rollup's manager may bind other custom data.
func checkLiveContext(h *Header, l *types.Log, e *protocol.BatchPosted, tx types.Transaction, b *protocol.ProofSystemBatch) error {
	data, err := protocol.SettlementCustomData(b.BlockNumber, h.Time, h.ParentHash)
	if err != nil {
		return fmt.Errorf("%w: block %d: %w", ErrUnsupported, h.Number, err)
	}
	var sender common.Address
	if b.BindMsgSenderInPublicInput {
		from, err := tx.Sender(*types.LatestSignerForChainID(tx.GetChainID()))
		if err != nil {
			return fmt.Errorf("%w: transaction %x of block %d: %w", ErrUndecodable, l.TxHash, h.Number, err)
		}
		sender = from.Value()
	}
	n := len(b.RollupIDsWithProofSystems)
	p, err := protocol.NewPublicInputs(b, tx.GetBlobHashes(), slices.Repeat([][]byte{data}, n), make([][]common.Hash, n), sender)
	if err != nil {
		return fmt.Errorf("%w: transaction %x of block %d: %w", ErrUnsupported, l.TxHash, h.Number, err)
	}
	if got := p.SharedHash(); got != e.SharedPublicInput {
		class := ErrUndecodable
		if n > 1 {
			class = ErrUnsupported
		}
		return fmt.Errorf("%w: transaction %x of block %d commits to %x, its header (time %d, parent %x) gives %x",
			class, l.TxHash, h.Number, e.SharedPublicInput, h.Time, h.ParentHash, got)
	}
	return nil
}
