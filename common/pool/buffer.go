package pool

import "github.com/valyala/bytebufferpool"

type ByteBuffer struct {
	*bytebufferpool.ByteBuffer
}

func (b ByteBuffer) Get(pos int) byte {
	return b.B[pos]
}

func (b ByteBuffer) SetBitPos(pos uint64) {
	b.B[pos/8] |= 0x80 >> (pos % 8)
}

func (b ByteBuffer) SetBit8Pos(pos uint64) {
	b.B[pos/8] |= 0xFF >> (pos % 8)
	b.B[pos/8+1] |= ^(0xFF >> (pos % 8))
}

func (b ByteBuffer) CodeSegment(pos uint64) bool {
	return b.B[pos/8]&(0x80>>(pos%8)) == 0
}
