package pool

import (
	"fmt"
	"math/bits"
)

const MaxPoolPow = 20
const MinPoolPow = 4

const PreAllocItems = 32 // preallocate some buffers

var pools = make([]*pool, MaxPoolPow+1)

func init() {
	// init chunkPools
	for i := MinPoolPow; i <= MaxPoolPow; i++ {
		if i%2 == 1 {
			continue
		}
		bufSize := uint(1 << i)
		idx := poolPutIdx(bufSize)
		pool := newPool(bufSize)
		for i := 0; i < PreAllocItems; i++ {
			pool.Put(pool.Get())
		}

		pools[idx] = pool
	}
}

// Calculate "nearest power 2 from bottom", then devide it to 2 - just to reduce amount of pools
// in result we will have separate pools for next power of 2: MinPoolPow, MinPoolPow + 2, MinPoolPow + 4, ..., MaxPoolPow
func poolPutIdx(n uint) int {
	n >>= MinPoolPow + 1
	lowPowerOf2 := bits.Len64(uint64(n))
	if lowPowerOf2 > MaxPoolPow-MinPoolPow {
		lowPowerOf2 = MaxPoolPow - MinPoolPow
	}
	return lowPowerOf2 / 2
}

func poolGetIdx(n uint) int {
	n--
	n >>= MinPoolPow - 1
	lowPowerOf2 := bits.Len64(uint64(n))
	if lowPowerOf2 > MaxPoolPow-MinPoolPow {
		lowPowerOf2 = MaxPoolPow - MinPoolPow
	}
	return lowPowerOf2 / 2
}

func GetBuffer(size uint) *ByteBuffer {
	pp := pools[poolGetIdx(size)].Get()
	if uint(cap(pp.B)) < size {
		fmt.Printf("why?: %d, %d\n", cap(pp.B), size)
	}
	pp.B = pp.B[:size]
	return pp
}

func GetBufferZeroed(size uint) *ByteBuffer {
	pp := GetBuffer(size)
	for i := range pp.B {
		pp.B[i] = 0
	}
	return pp
}

func PutBuffer(p *ByteBuffer) {
	if p == nil || cap(p.B) == 0 {
		return
	}
	pools[poolPutIdx(uint(cap(p.B)))].Put(p)
}
