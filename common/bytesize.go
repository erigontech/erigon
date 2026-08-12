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

package common

// Binary multipliers for byte sizes. They are untyped on purpose: array
// lengths, buffer sizes and consensus parameters need a plain int, which is
// where datasize.ByteSize does not fit.
const (
	Kibi = 1 << 10
	Mebi = 1 << 20
	Gibi = 1 << 30
	Tebi = 1 << 40
)
