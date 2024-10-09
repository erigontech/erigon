/*
Copyright 2021 Erigon contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bitmapdb

import (
	"github.com/RoaringBitmap/roaring/roaring64"
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
