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
	"bytes"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
)

// SpanVersionV0 selects native_block_span_v0 as the first byte of the
// rollup's operations.
const SpanVersionV0 byte = 0x00

// MaxExtraData is the cap on a block's extraData.
const MaxExtraData = 32

// Error classes of Rollup0 Appendix D.4.
var (
	ErrSpanEmpty        = errors.New("span without a version byte")
	ErrSpanVersion      = errors.New("unsupported span version")
	ErrSpanTruncated    = errors.New("truncated span")
	ErrSpanVarint       = errors.New("non-canonical uvarint32")
	ErrSpanNoBlocks     = errors.New("span covers no blocks")
	ErrSpanImplausible  = errors.New("count exceeds what the remaining bytes can encode")
	ErrSpanRunCoverage  = errors.New("runs do not cover the span exactly")
	ErrSpanNonMaximal   = errors.New("adjacent runs repeat a value")
	ErrSpanExtraData    = errors.New("extraData longer than 32 bytes")
	ErrSpanEmptyTx      = errors.New("zero-length transaction")
	ErrSpanTrailingByte = errors.New("trailing bytes after the span")
)

// Span is a decoded native_block_span_v0, kept columnar: the transaction
// counts delimit the blocks within Transactions.
type Span struct {
	// TxCounts holds each block's pure-L2 transaction count; its length is
	// the number of blocks.
	TxCounts      []uint32
	Beneficiaries []common.Address
	ExtraData     [][]byte
	// Transactions are the pure-L2 transactions in block order, each in its
	// EIP-2718 encoding.
	Transactions [][]byte
}

// DecodeSpan decodes operations = 0x00 ‖ native_block_span_v0 (Rollup0
// Appendix D.4). Every count is bounded by the remaining bytes before
// anything is allocated for it, and the payload must be consumed exactly.
func DecodeSpan(operations []byte) (*Span, error) {
	if len(operations) == 0 {
		return nil, ErrSpanEmpty
	}
	if operations[0] != SpanVersionV0 {
		return nil, fmt.Errorf("%w: %#02x", ErrSpanVersion, operations[0])
	}
	r := spanReader{buf: bytes.Clone(operations[1:])}
	blocks, err := r.uvarint32("block_count")
	if err != nil {
		return nil, err
	}
	if blocks == 0 {
		return nil, ErrSpanNoBlocks
	}
	// Each block costs at least its one-byte transaction count.
	if uint64(blocks) > uint64(r.remaining()) {
		return nil, fmt.Errorf("%w: %d blocks in %d bytes", ErrSpanImplausible, blocks, r.remaining())
	}
	s := &Span{TxCounts: make([]uint32, blocks)}
	var txs uint64
	for i := range s.TxCounts {
		if s.TxCounts[i], err = r.uvarint32("pure_transaction_counts"); err != nil {
			return nil, err
		}
		txs += uint64(s.TxCounts[i])
	}
	if s.Beneficiaries, err = decodeRuns(&r, blocks, "beneficiary", func() (common.Address, error) {
		b, err := r.take("beneficiary", 20)
		if err != nil {
			return common.Address{}, err
		}
		return common.Address(b), nil
	}, func(a, b common.Address) bool { return a == b }); err != nil {
		return nil, err
	}
	if s.ExtraData, err = decodeRuns(&r, blocks, "extra_data", func() ([]byte, error) {
		n, err := r.take("extra_data length", 1)
		if err != nil {
			return nil, err
		}
		if n[0] > MaxExtraData {
			return nil, fmt.Errorf("%w: %d", ErrSpanExtraData, n[0])
		}
		return r.take("extra_data", int(n[0]))
	}, bytes.Equal); err != nil {
		return nil, err
	}
	// Each transaction costs a length byte and at least one content byte.
	if txs > uint64(r.remaining()/2) {
		return nil, fmt.Errorf("%w: %d transactions in %d bytes", ErrSpanImplausible, txs, r.remaining())
	}
	lengths := make([]uint32, txs)
	var total uint64
	for i := range lengths {
		if lengths[i], err = r.uvarint32("pure_transaction_lengths"); err != nil {
			return nil, err
		}
		if lengths[i] == 0 {
			return nil, fmt.Errorf("%w: transaction %d", ErrSpanEmptyTx, i)
		}
		total += uint64(lengths[i])
	}
	switch rest := uint64(r.remaining()); {
	case total > rest:
		return nil, fmt.Errorf("%w: %d transaction bytes declared, %d left", ErrSpanTruncated, total, rest)
	case total < rest:
		return nil, fmt.Errorf("%w: %d", ErrSpanTrailingByte, rest-total)
	}
	s.Transactions = make([][]byte, txs)
	for i, n := range lengths {
		s.Transactions[i], _ = r.take("pure_transaction_bytes", int(n))
	}
	return s, nil
}

// decodeRuns expands a column of (uvarint32 run length, value) runs into one
// value per block. Runs are positive, maximal and cover exactly n blocks.
func decodeRuns[T any](r *spanReader, n uint32, column string, value func() (T, error), equal func(a, b T) bool) ([]T, error) {
	out := make([]T, 0, n)
	for uint32(len(out)) < n {
		run, err := r.uvarint32(column + " run length")
		if err != nil {
			return nil, err
		}
		if run == 0 || uint64(len(out))+uint64(run) > uint64(n) {
			return nil, fmt.Errorf("%w: %s", ErrSpanRunCoverage, column)
		}
		v, err := value()
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && equal(out[len(out)-1], v) {
			return nil, fmt.Errorf("%w: %s", ErrSpanNonMaximal, column)
		}
		for range run {
			out = append(out, v)
		}
	}
	return out, nil
}

type spanReader struct {
	buf []byte
	pos int
}

func (r *spanReader) remaining() int { return len(r.buf) - r.pos }

func (r *spanReader) take(field string, n int) ([]byte, error) {
	if n > r.remaining() {
		return nil, fmt.Errorf("%w: %s", ErrSpanTruncated, field)
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

// uvarint32 reads a shortest-form protobuf varint of at most five bytes
// holding a uint32: every value has exactly one encoding.
func (r *spanReader) uvarint32(field string) (uint32, error) {
	var v uint64
	for i := range 5 {
		b, err := r.take(field, 1)
		if err != nil {
			return 0, err
		}
		group := uint64(b[0] & 0x7f)
		if i == 4 && group > 0x0f {
			return 0, fmt.Errorf("%w: %s exceeds uint32", ErrSpanVarint, field)
		}
		v |= group << (7 * i)
		if b[0]&0x80 == 0 {
			if i > 0 && group == 0 {
				return 0, fmt.Errorf("%w: %s is padded", ErrSpanVarint, field)
			}
			return uint32(v), nil
		}
	}
	return 0, fmt.Errorf("%w: %s longer than five bytes", ErrSpanVarint, field)
}
