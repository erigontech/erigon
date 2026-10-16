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
// Package da decodes the data-availability payload of EEZ batches: the EEZ
// message stream of eez-core-protocol docs/blobs/BLOB_FORMAT_SPEC.md and the
// rollup operations it carries.
package da

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// StreamVersion is the stream version byte this package decodes.
const StreamVersion byte = 0x00

const (
	// FieldElementsPerBlob is the number of field elements in an EIP-4844 blob.
	FieldElementsPerBlob = 4096
	// BlobSize is the size of an EIP-4844 blob.
	BlobSize = 32 * FieldElementsPerBlob
	// BlobDataSize is the number of stream bytes a blob carries.
	BlobDataSize = 31 * FieldElementsPerBlob
)

// MessageType selects the layout of a message.
type MessageType byte

const (
	CloseBlobStream                MessageType = 1
	RollupOperation                MessageType = 2
	InitiateCrossRollupTransaction MessageType = 3
	Call                           MessageType = 4
	StaticCall                     MessageType = 5
	ReturnSuccess                  MessageType = 6
	ReturnFail                     MessageType = 7
	Snapshot                       MessageType = 8
	Revert                         MessageType = 9
	FinishCrossRollupTransaction   MessageType = 10
)

var (
	ErrBlobEncoding = errors.New("invalid blob encoding")
	ErrVersion      = errors.New("unsupported stream version")
	ErrMessageType  = errors.New("invalid message type")
	ErrTruncated    = errors.New("truncated stream")
	ErrLength       = errors.New("invalid length prefix")
	ErrBracket      = errors.New("unbalanced bracket")
	ErrPlacement    = errors.New("message out of place")
	ErrBlobClose    = errors.New("misplaced or missing CloseBlobStream")
)

// Message is one decoded message; fields its type does not carry are zero.
type Message struct {
	Type MessageType
	// RollupID is rollup_id for RollupOperation and
	// InitiateCrossRollupTransaction, to_rollup for Call and StaticCall.
	RollupID uint64
	// FromRollup is the executing rollup a Call or StaticCall leaves, implied
	// by the context stack.
	FromRollup  uint64
	FromAddress common.Address
	ToAddress   common.Address
	Value       uint256.Int // Call only
	Gas         uint64
	// Data is operations, tx_data, data or return_data.
	Data []byte
}

// BlobData returns the stream bytes a blob carries. EIP-4844 serializes each
// field element big-endian while the stream reads it as a little-endian
// scalar whose most significant byte is zero: its 31 stream bytes are the
// serialized bytes from the last back to the second, and the first must be
// zero.
func BlobData(blob []byte) ([]byte, error) {
	if len(blob) != BlobSize {
		return nil, fmt.Errorf("%w: %d-byte blob", ErrBlobEncoding, len(blob))
	}
	out := make([]byte, 0, BlobDataSize)
	for i := 0; i < len(blob); i += 32 {
		if blob[i] != 0 {
			return nil, fmt.Errorf("%w: field element %d exceeds 248 bits", ErrBlobEncoding, i/32)
		}
		for j := i + 31; j > i; j-- {
			out = append(out, blob[j])
		}
	}
	return out, nil
}

// Decode decodes the message stream of a batch, its blobs in order followed
// by its callData, and checks every validity condition of the format: the
// stream is rejected whole on the first violation. With blobs, the version
// byte opens the blob portion and CloseBlobStream ends it once; the bytes
// after it are padding and the stream resumes in callData. Without blobs,
// callData holds the whole stream and CloseBlobStream is invalid.
// CloseBlobStream is not returned.
func Decode(blobs [][]byte, callData []byte) ([]Message, error) {
	var d decoder
	callData = bytes.Clone(callData)
	if len(blobs) == 0 {
		rest, err := version(callData)
		if err != nil {
			return nil, err
		}
		if err := d.run(rest, false); err != nil {
			return nil, err
		}
		return d.finish()
	}
	data := make([]byte, 0, len(blobs)*BlobDataSize)
	for i, blob := range blobs {
		b, err := BlobData(blob)
		if err != nil {
			return nil, fmt.Errorf("blob %d: %w", i, err)
		}
		data = append(data, b...)
	}
	rest, err := version(data)
	if err != nil {
		return nil, err
	}
	if err := d.run(rest, true); err != nil {
		return nil, err
	}
	if !d.closed {
		return nil, fmt.Errorf("%w: blob portion ends without it", ErrBlobClose)
	}
	if err := d.run(callData, false); err != nil {
		return nil, err
	}
	return d.finish()
}

func version(stream []byte) ([]byte, error) {
	if len(stream) == 0 {
		return nil, fmt.Errorf("%w: empty stream", ErrTruncated)
	}
	if stream[0] != StreamVersion {
		return nil, fmt.Errorf("%w: %#02x", ErrVersion, stream[0])
	}
	return stream[1:], nil
}

type decoder struct {
	buf []byte
	pos int
	// stack holds the executing rollup of the open transaction and calls.
	stack []uint64
	// snapshots holds the stack depth each open Snapshot was taken at.
	snapshots []int
	closed    bool
	msgs      []Message
}

// run decodes the messages of one portion. In the blob portion it stops at
// CloseBlobStream, skipping the padding that follows.
func (d *decoder) run(buf []byte, blob bool) error {
	d.buf, d.pos = buf, 0
	for d.pos < len(d.buf) {
		at := d.pos
		t := MessageType(d.buf[d.pos])
		d.pos++
		if t == CloseBlobStream {
			if !blob {
				return fmt.Errorf("%w: in callData at %d", ErrBlobClose, at)
			}
			d.closed = true
			return nil
		}
		m, err := d.message(t)
		if err != nil {
			return fmt.Errorf("message %d at %d: %w", len(d.msgs), at, err)
		}
		d.msgs = append(d.msgs, m)
	}
	return nil
}

func (d *decoder) finish() ([]Message, error) {
	if len(d.stack) != 0 || len(d.snapshots) != 0 {
		return nil, fmt.Errorf("%w: stream ends inside a transaction", ErrBracket)
	}
	return d.msgs, nil
}

func (d *decoder) message(t MessageType) (Message, error) {
	m := Message{Type: t}
	var err error
	switch t {
	case RollupOperation:
		if len(d.stack) != 0 {
			return m, fmt.Errorf("%w: RollupOperation inside a transaction", ErrPlacement)
		}
		if m.RollupID, err = d.u64(); err != nil {
			return m, err
		}
		m.Data, err = d.bytes()
	case InitiateCrossRollupTransaction:
		if len(d.stack) != 0 {
			return m, fmt.Errorf("%w: nested transaction", ErrBracket)
		}
		if m.RollupID, err = d.u64(); err != nil {
			return m, err
		}
		if m.Data, err = d.bytes(); err != nil {
			return m, err
		}
		d.stack = append(d.stack, m.RollupID)
	case Call, StaticCall:
		if len(d.stack) == 0 {
			return m, fmt.Errorf("%w: call outside a transaction", ErrPlacement)
		}
		m.FromRollup = d.stack[len(d.stack)-1]
		if err := d.call(&m); err != nil {
			return m, err
		}
		d.stack = append(d.stack, m.RollupID)
	case ReturnSuccess, ReturnFail:
		// The transaction's root and the returning call are on the stack, and
		// the return must not leave an open Snapshot's frame.
		if len(d.stack) < 2 {
			return m, fmt.Errorf("%w: return without a call", ErrBracket)
		}
		if n := len(d.snapshots); n > 0 && d.snapshots[n-1] > len(d.stack)-1 {
			return m, fmt.Errorf("%w: return leaves a Snapshot's frame", ErrBracket)
		}
		if m.Data, err = d.bytes(); err != nil {
			return m, err
		}
		d.stack = d.stack[:len(d.stack)-1]
	case Snapshot:
		if len(d.stack) == 0 {
			return m, fmt.Errorf("%w: Snapshot outside a transaction", ErrPlacement)
		}
		d.snapshots = append(d.snapshots, len(d.stack))
	case Revert:
		n := len(d.snapshots)
		if n == 0 || d.snapshots[n-1] != len(d.stack) {
			return m, fmt.Errorf("%w: Revert without a Snapshot at this depth", ErrBracket)
		}
		d.snapshots = d.snapshots[:n-1]
	case FinishCrossRollupTransaction:
		if len(d.stack) != 1 || len(d.snapshots) != 0 {
			return m, fmt.Errorf("%w: finish with %d frames and %d snapshots open", ErrBracket, len(d.stack), len(d.snapshots))
		}
		d.stack = d.stack[:0]
	default:
		return m, fmt.Errorf("%w: %d", ErrMessageType, t)
	}
	return m, err
}

func (d *decoder) call(m *Message) error {
	var err error
	if m.RollupID, err = d.u64(); err != nil {
		return err
	}
	from, err := d.take(20)
	if err != nil {
		return err
	}
	to, err := d.take(20)
	if err != nil {
		return err
	}
	m.FromAddress, m.ToAddress = common.Address(from), common.Address(to)
	if m.Type == Call {
		value, err := d.take(32)
		if err != nil {
			return err
		}
		be := slices.Clone(value)
		slices.Reverse(be)
		m.Value.SetBytes32(be)
	}
	if m.Gas, err = d.u64(); err != nil {
		return err
	}
	m.Data, err = d.bytes()
	return err
}

func (d *decoder) take(n int) ([]byte, error) {
	if n > len(d.buf)-d.pos {
		return nil, ErrTruncated
	}
	b := d.buf[d.pos : d.pos+n]
	d.pos += n
	return b, nil
}

func (d *decoder) u64() (uint64, error) {
	b, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// bytes reads a field prefixed with a protobuf varint length of at most five
// bytes holding a uint32. Non-minimal prefixes are valid.
func (d *decoder) bytes() ([]byte, error) {
	var n uint64
	for i := 0; ; i++ {
		if i == 5 {
			return nil, fmt.Errorf("%w: longer than five bytes", ErrLength)
		}
		b, err := d.take(1)
		if err != nil {
			return nil, err
		}
		group := uint64(b[0] & 0x7f)
		if i == 4 && group > 0x0f {
			return nil, fmt.Errorf("%w: exceeds uint32", ErrLength)
		}
		n |= group << (7 * i)
		if b[0]&0x80 == 0 {
			break
		}
	}
	if n > uint64(len(d.buf)-d.pos) {
		return nil, ErrTruncated
	}
	return d.take(int(n))
}
