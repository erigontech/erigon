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
