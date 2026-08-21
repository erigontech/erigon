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

func (g *Getter) EnableAsyncLiteralWarm() {
	g.literalWarmer = (*Getter).warmLiteral
}

func shouldWarmLiteral(offset, length uint64) bool {
	if length == 0 {
		return false
	}
	end := offset + length
	if end < offset {
		return false
	}
	page := uint64(pageSize)
	return offset/page != (end-1)/page
}

func (g *Getter) warmLiteral(offset, length uint64) {
	if !shouldWarmLiteral(offset, length) || offset+length > math.MaxInt64 {
		return
	}
	for length > 0 {
		chunk := min(length, uint64(iouring.WarmBufSize))
		iouring.WarmOne(int(g.d.f.Fd()), int64(offset), int(chunk))
		offset += chunk
		length -= chunk
	}
}
