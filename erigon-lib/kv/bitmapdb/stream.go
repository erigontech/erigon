// Copyright 2021 The Erigon Authors
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
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type BitmapStream struct {
	bm *roaring64.Bitmap
	it roaring64.IntPeekable64
}

func NewBitmapStream(bm *roaring64.Bitmap) *BitmapStream {
	return &BitmapStream{bm: bm, it: bm.Iterator()}
}
func (it *BitmapStream) HasNext() bool                        { return it.it.HasNext() }
func (it *BitmapStream) Close()                               { ReturnToPool64(it.bm) }
func (it *BitmapStream) Next() (uint64, error)                { return it.it.Next(), nil }
func (it *BitmapStream) ToBitmap() (*roaring64.Bitmap, error) { return it.bm, nil }
