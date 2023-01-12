package bitmapdb

import (
	"github.com/RoaringBitmap/roaring"
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
func (it *BitmapStream) Close()                               {}
func (it *BitmapStream) Next() (uint64, error)                { return it.it.Next(), nil }
func (it *BitmapStream) ToBitmap() (*roaring64.Bitmap, error) { return it.bm, nil }

func CastBitmapTo64(in *roaring.Bitmap) *roaring64.Bitmap {
	if in == nil {
		return nil
	}
	bm := NewBitmap64()
	for _, v := range in.ToArray() {
		bm.Add(uint64(v))
	}
	return bm
}
