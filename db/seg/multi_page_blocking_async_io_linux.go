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

//go:build linux

package seg

import (
	"math"

	"github.com/erigontech/erigon/common/iouring"
)

func (g *Getter) EnableMultiPageBlockingAsyncIO() {
	g.multiPageBlockingAsyncRead = (*Getter).readMultiPageLiteral
	g.multiPageReadThreshold = uint64(pageSize)
}

func multiPageBlockingAsyncReadRange(offset, length uint64) (uint64, uint64, bool) {
	page := uint64(pageSize)
	if length <= page {
		return 0, 0, false
	}
	end := offset + length
	if end < offset {
		return 0, 0, false
	}
	// The metadata walk has already touched the page containing the literal start.
	if remainder := offset % page; remainder != 0 {
		offset += page - remainder
	}
	if offset >= end {
		return 0, 0, false
	}
	return offset, end - offset, true
}

func (g *Getter) readMultiPageLiteral(offset, length uint64) {
	offset, length, ok := multiPageBlockingAsyncReadRange(offset, length)
	if !ok || offset+length > math.MaxInt64 {
		return
	}
	for length > 0 {
		chunk := min(length, uint64(iouring.MaxReadSize))
		g.d.blockingAsyncRead(int64(offset), int(chunk))
		offset += chunk
		length -= chunk
	}
}
