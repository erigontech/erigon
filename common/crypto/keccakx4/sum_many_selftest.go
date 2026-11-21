package keccakx4

import keccak "github.com/erigontech/fastkeccak"

// x4SelfTest reports whether permute hashes messages of every block-count pattern a lane can
// see, including lanes that finish at different times, the same as Sum256.
func x4SelfTest(permute func(a *[100]uint64, blocks *[4]*byte)) bool {
	sizes := []int{0, 1, rate - 1, rate, rate + 1, 3*rate + 7, 2 * rate, 55, 700}
	msgs := make([][]byte, len(sizes))
	for i, n := range sizes {
		msgs[i] = make([]byte, n)
		for j := range msgs[i] {
			msgs[i][j] = byte(j*31 + i)
		}
	}
	out := make([][32]byte, len(msgs))
	sumManyX4(permute, msgs, out)
	for i, msg := range msgs {
		if out[i] != keccak.Sum256(msg) {
			return false
		}
	}
	return true
}
