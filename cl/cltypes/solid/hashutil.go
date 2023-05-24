package solid

type hashBuf struct {
	buf []byte
}

func (arr *hashBuf) makeBuf(size int) {
	diff := size - len(arr.buf)
	if diff > 0 {
		arr.buf = append(arr.buf, make([]byte, diff)...)
	}
	arr.buf = arr.buf[:size]
}

func getDepth(v uint64) uint8 {
	// If there are 0 or 1 nodes, the depth is 0.
	if v <= 1 {
		return 0
	}

	// Initialize the depth to 0.
	depth := uint8(0)

	// Divide the number of nodes by 2 until it is less than or equal to 1.
	// The number of iterations is the depth of the tree.
	for v > 1 {
		v >>= 1
		depth++
	}

	return depth
}
