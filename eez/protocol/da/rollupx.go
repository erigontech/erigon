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

package da

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MaxRollupXBlockTxs is the largest pure-L2 transaction count of a block in
// a RollupX span.
const MaxRollupXBlockTxs = 16_383

// MaxRollupXSpanBlocks bounds the blocks a RollupX span expands to. One
// token admits a run of 2^32−1 empty blocks; a follower executes each, so it
// refuses a longer span instead of expanding it.
const MaxRollupXSpanBlocks = 1 << 24

var (
	ErrSpanCountToken    = errors.New("invalid transaction-count token")
	ErrSpanTooLong       = errors.New("span beyond the blocks a follower expands")
	ErrSpanUnsupportedTx = errors.New("system or blob transaction in the span")
)

// Transaction types a RollupX span cannot carry.
const (
	systemTxType = 0x76
	blobTxType   = 0x03
)

// DecodeRollupXSpan decodes the operations of an eez-rollupX ChainOperation:
// a V0 span whose transaction counts are tokens, not one uvarint32 per block.
// A token is 0x00 for one empty block, 0x01–0x7f for that many transactions
// in one block, 0x80–0xbf and a second byte for 128 to 16,383 transactions in
// 14 bits, 0xc0–0xfe for 2 to 64 empty blocks, or 0xff and a uvarint32 for at
// least 65 empty blocks. Every encoding is the shortest, and two empty runs
// never follow each other.
func DecodeRollupXSpan(operations []byte) (*Span, error) {
	if len(operations) == 0 {
		return nil, ErrSpanEmpty
	}
	if operations[0] != SpanVersionV0 {
		return nil, fmt.Errorf("%w: %#02x", ErrSpanVersion, operations[0])
	}
	r := spanReader{buf: operations[1:]}
	blocks, err := r.uvarint32("block_count")
	if err != nil {
		return nil, err
	}
	switch {
	case blocks == 0:
		return nil, ErrSpanNoBlocks
	case blocks > MaxRollupXSpanBlocks:
		return nil, fmt.Errorf("%w: %d blocks", ErrSpanTooLong, blocks)
	}
	s := &Span{TxCounts: make([]uint32, 0, blocks)}
	var txs uint64
	var previousEmpty bool
	for uint32(len(s.TxCounts)) < blocks {
		count, run, err := r.countToken()
		if err != nil {
			return nil, err
		}
		empty := run > 0
		switch {
		case empty && previousEmpty:
			return nil, fmt.Errorf("%w: adjacent empty runs", ErrSpanCountToken)
		case uint64(len(s.TxCounts))+uint64(max(run, 1)) > uint64(blocks):
			return nil, fmt.Errorf("%w: pure_transaction_counts", ErrSpanRunCoverage)
		}
		previousEmpty = empty
		if !empty {
			s.TxCounts = append(s.TxCounts, count)
			txs += uint64(count)
			continue
		}
		for range run {
			s.TxCounts = append(s.TxCounts, 0)
		}
	}
	if err := s.decodeColumns(&r, blocks, txs); err != nil {
		return nil, err
	}
	for i, tx := range s.Transactions {
		if tx[0] == systemTxType || tx[0] == blobTxType {
			return nil, fmt.Errorf("%w: transaction %d of type %#02x", ErrSpanUnsupportedTx, i, tx[0])
		}
	}
	return s, nil
}

// countToken reads a transaction-count token: count transactions in one
// block, or a run of empty blocks.
func (r *spanReader) countToken() (count, run uint32, err error) {
	b, err := r.take("pure_transaction_counts", 1)
	if err != nil {
		return 0, 0, err
	}
	switch t := b[0]; {
	case t == 0:
		return 0, 1, nil
	case t < 0x80:
		return uint32(t), 0, nil
	case t < 0xc0:
		lo, err := r.take("pure_transaction_counts", 1)
		if err != nil {
			return 0, 0, err
		}
		n := uint32(t&0x3f)<<8 | uint32(lo[0])
		if n < 0x80 {
			return 0, 0, fmt.Errorf("%w: count %d in two bytes", ErrSpanCountToken, n)
		}
		return n, 0, nil
	case t < 0xff:
		return 0, uint32(t-0xc0) + 2, nil
	default:
		n, err := r.uvarint32("pure_transaction_counts run")
		if err != nil {
			return 0, 0, err
		}
		if n < 65 {
			return 0, 0, fmt.Errorf("%w: run of %d in the long form", ErrSpanCountToken, n)
		}
		return 0, n, nil
	}
}

// EncodeRollupX returns the RollupX encoding of s, the one DecodeRollupXSpan
// admits.
func (s *Span) EncodeRollupX() []byte {
	out := []byte{SpanVersionV0}
	out = binary.AppendUvarint(out, uint64(len(s.TxCounts)))
	for i := 0; i < len(s.TxCounts); {
		if n := s.TxCounts[i]; n > 0 {
			if n < 0x80 {
				out = append(out, byte(n))
			} else {
				out = append(out, 0x80|byte(n>>8), byte(n))
			}
			i++
			continue
		}
		j := i + 1
		for j < len(s.TxCounts) && s.TxCounts[j] == 0 {
			j++
		}
		switch run := j - i; {
		case run == 1:
			out = append(out, 0)
		case run <= 64:
			out = append(out, 0xc0+byte(run-2))
		default:
			out = binary.AppendUvarint(append(out, 0xff), uint64(run))
		}
		i = j
	}
	return s.appendColumns(out)
}
