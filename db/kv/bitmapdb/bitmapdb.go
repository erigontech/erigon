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

package bitmapdb

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

var roaring64Pool = sync.Pool{
	New: func() any {
		return roaring64.New()
	},
}

func NewBitmap64() *roaring64.Bitmap {
	a := roaring64Pool.Get().(*roaring64.Bitmap)
	a.Clear()
	return a
}
func ReturnToPool64(a *roaring64.Bitmap) {
	if a == nil {
		return
	}
	roaring64Pool.Put(a)
}
