// Copyright 2024 The Erigon Authors
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

package rlphacks

import (
	"io"
)

// RlpSerializableTape is an abstraction for a tape that allows reading a sequence of
// RlpSerializable values.
type RlpSerializableTape interface {
	Next() (RlpSerializable, error)
}

// RlpSerializable is a value that can be double-RLP coded.
type RlpSerializable interface {
	ToDoubleRLP(io.Writer, []byte) error
	DoubleRLPLen() int
	RawBytes() []byte
}
