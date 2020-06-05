package pool

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
		1 << 11,
		1 << 12,
		1 << 13,
		1 << 14,
		1 << 15,
		1 << 16,
		1 << 17,
		1 << 18,
		1 << 19,
		1 << 20,
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
	for _, n := range chunkSizeClasses {

		for i := 0; i < preAlloc; i++ {
			PutBuffer(GetBuffer(n))
		}
	}
}

func GetBuffer(size uint) *ByteBuffer {
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

	for i, n := range chunkSizeClasses {
		if uint(cap(p.B)) <= n {
			p.B = p.B[:0]
			chunkPools[i].pool.Put(p)
			break
		}
	}
}
