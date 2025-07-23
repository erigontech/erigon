// Copyright 2022 The Erigon Authors
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

package state

import (
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/seg"
)

// SegReaderWrapper wraps seg.ReaderI to satisfy stream.KV interface
type SegReaderWrapper struct {
	reader seg.ReaderI
}

// NewSegReaderWrapper creates a new wrapper for seg.ReaderI to satisfy stream.KV interface
func NewSegReaderWrapper(reader seg.ReaderI) stream.KV {
	return &SegReaderWrapper{reader: reader}
}

// Next returns key and value by calling the underlying getter twice
func (w *SegReaderWrapper) Next() ([]byte, []byte, error) {
	if !w.reader.HasNext() {
		return nil, nil, stream.ErrIteratorExhausted
	}

	// First call: get the key
	key, _ := w.reader.Next(nil)

	// Second call: get the value
	var value []byte
	if w.reader.HasNext() {
		value, _ = w.reader.Next(nil)
	}

	return key, value, nil
}

// HasNext delegates to the underlying reader
func (w *SegReaderWrapper) HasNext() bool {
	return w.reader.HasNext()
}

// Close is a no-op as seg.ReaderI doesn't have Close method
func (w *SegReaderWrapper) Close() {
	// No-op
}
