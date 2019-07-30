package pool

import (
	"github.com/valyala/bytebufferpool"
)

var (
	chunkSizeClasses = []uint{
		8,
		64,
		75,
		128,
		192,
		1 << 8,
		1 << 9,
		1 << 10,
		2 << 10,
		4 << 10,
		8 << 10,
		16 << 10,
	}
	chunkPools []*pool
)

func init() {
	// init chunkPools
	for _, chunkSize := range chunkSizeClasses {
		chunkPools = append(chunkPools, newPool(chunkSize))
	}

	// preallocate some buffers
	const preAlloc = 32
	const lastSmallChunkIndex = 5
	for i, n := range chunkSizeClasses {
		if i > lastSmallChunkIndex {
			break
		}

		for i := 0; i < preAlloc; i++ {
			PutBuffer(NewValue(make([]byte, 0, n)))
		}
	}
}

func GetBuffer(size uint) *bytebufferpool.ByteBuffer {
	var i int
	for i = 0; i < len(chunkSizeClasses)-1; i++ {
		if size <= chunkSizeClasses[i] {
			break
		}
	}

	pp := chunkPools[i].Get()

	if capB := cap(pp.B); uint(capB) < size {
		if capB == 0 {
			_ = pp.WriteByte(0)
		}
		if capB != 0 {
			pp.B = pp.B[:capB]
			_, _ = pp.Write(make([]byte, size-uint(capB)))
		}
	}

	pp.B = pp.B[:size]

	return pp
}

func PutBuffer(p *bytebufferpool.ByteBuffer) {
	if p == nil || cap(p.B) == 0 {
		return
	}

	for i, n := range chunkSizeClasses {
		if uint(cap(p.B)) <= n {
			chunkPools[i].Put(p)
			break
		}
	}
}

func NewValue(b []byte) *bytebufferpool.ByteBuffer {
	return &bytebufferpool.ByteBuffer{
		B: b,
	}
}
