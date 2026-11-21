// Package keccakx4 hashes batches of messages four at a time with AVX2 or AVX-512.
package keccakx4

import (
	"slices"

	keccak "github.com/erigontech/fastkeccak"
)

// SumMany256 sets out[i] to the Keccak-256 hash of msgs[i]; out must be at least as long as
// msgs. On amd64 with AVX2 or AVX-512 it hashes four messages at a time, which is faster than
// calling Sum256 in a loop when there are several messages; elsewhere it is that loop.
func SumMany256(msgs [][]byte, out [][32]byte) {
	_ = out[:len(msgs)]
	if x4 == nil || len(msgs) < 4 {
		for i, msg := range msgs {
			out[i] = keccak.Sum256(msg)
		}
		return
	}
	sumManyX4(x4, msgs, out)
}

// x4 absorbs one rate block of each of four messages into a four-lane state and permutes it.
// It is nil when no four-lane implementation is available or the startup self-test failed.
var x4 func(a *[100]uint64, blocks *[4]*byte)

// rate is the Keccak-256 block size in bytes.
const rate = 136

var zeroBlock [rate]byte

// sumManyX4 runs four messages at once; a lane that finishes takes the next message, so
// messages of different lengths do not leave lanes idle. The longest messages go first so
// the lanes run out together.
func sumManyX4(permute func(a *[100]uint64, blocks *[4]*byte), msgs [][]byte, out [][32]byte) {
	order := longestFirst(msgs)
	var (
		a      [100]uint64
		blocks [4]*byte
		padBuf [4 * rate]byte
		cur    [4]int
		pos    [4]int
		next   int
		active int
	)
	take := func(lane int) {
		if next == len(order) {
			cur[lane] = -1
			blocks[lane] = &zeroBlock[0]
			return
		}
		cur[lane], pos[lane] = order[next], 0
		next++
		active++
	}
	for lane := range 4 {
		take(lane)
	}
	for active > 0 {
		for lane := range 4 {
			if cur[lane] < 0 {
				continue
			}
			rest := msgs[cur[lane]][pos[lane]:]
			if len(rest) >= rate {
				blocks[lane] = &rest[0]
				continue
			}
			p := padBuf[lane*rate : (lane+1)*rate]
			clear(p)
			n := copy(p, rest)
			p[n] ^= 0x01
			p[rate-1] ^= 0x80
			blocks[lane] = &p[0]
		}
		permute(&a, &blocks)
		for lane := range 4 {
			if cur[lane] < 0 {
				continue
			}
			if len(msgs[cur[lane]])-pos[lane] >= rate {
				pos[lane] += rate
				continue
			}
			o := &out[cur[lane]]
			for k := range 4 {
				putUint64LE(o[8*k:], a[4*k+lane])
			}
			for k := range 25 {
				a[4*k+lane] = 0
			}
			active--
			take(lane)
		}
	}
}

func longestFirst(msgs [][]byte) []int {
	order := make([]int, len(msgs))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(i, j int) int { return len(msgs[j]) - len(msgs[i]) })
	return order
}

func putUint64LE(b []byte, v uint64) {
	_ = b[7]
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}
