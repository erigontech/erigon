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

package seqsig

import (
	"errors"
	"fmt"

	"github.com/golang/snappy"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

// MaxMessageSize caps the RLP of an envelope and its Snappy block.
const MaxMessageSize = 10 << 20

var (
	ErrTooLarge  = errors.New("message exceeds the size cap")
	ErrMalformed = errors.New("malformed message")
)

// wireHead and wireEnvelope fix the RLP layout
// [[rollupId, number, blockHash, parentHash, epochMs, counter], signature, block].
type wireHead struct {
	RollupID   uint64
	Number     uint64
	BlockHash  common.Hash
	ParentHash common.Hash
	EpochMs    uint64
	Counter    uint64
}

type wireEnvelope struct {
	Head      wireHead
	Signature []byte
	Block     []byte
}

// Encode returns the raw Snappy block of the RLP of e.
func Encode(e Envelope) ([]byte, error) {
	enc, err := rlp.EncodeToBytes(&wireEnvelope{
		Head:      wireHead(e.Head),
		Signature: e.Signature,
		Block:     e.Block,
	})
	if err != nil {
		return nil, err
	}
	if len(enc) > MaxMessageSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(enc))
	}
	out := snappy.Encode(nil, enc)
	if len(out) > MaxMessageSize {
		return nil, fmt.Errorf("%w: %d bytes compressed", ErrTooLarge, len(out))
	}
	return out, nil
}

// Decode is the inverse of Encode. The Snappy block and its uncompressed size
// are checked against the cap before anything is allocated.
func Decode(data []byte) (Envelope, error) {
	if len(data) > MaxMessageSize {
		return Envelope{}, fmt.Errorf("%w: %d bytes compressed", ErrTooLarge, len(data))
	}
	n, err := snappy.DecodedLen(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: snappy: %w", ErrMalformed, err)
	}
	if n > MaxMessageSize {
		return Envelope{}, fmt.Errorf("%w: %d bytes", ErrTooLarge, n)
	}
	enc, err := snappy.Decode(nil, data)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: snappy: %w", ErrMalformed, err)
	}
	var w wireEnvelope
	if err := rlp.DecodeBytes(enc, &w); err != nil {
		return Envelope{}, fmt.Errorf("%w: rlp: %w", ErrMalformed, err)
	}
	e := Envelope{Head: Head(w.Head), Signature: w.Signature}
	if len(w.Block) > 0 {
		e.Block = w.Block
	}
	return e, nil
}
