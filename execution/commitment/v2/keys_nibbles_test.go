package v2

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/commitment"
)

// maxAddrSearchIters bounds the brute-force address search helpers below so a
// broken search space (e.g. hash function change) produces a descriptive panic
// instead of an infinite hang. 1M iterations is well above the expected work:
// a single-nibble hit averages ~16 iters; a 16-bit shared-prefix hit averages
// ~65k, both comfortably under the cap.
const maxAddrSearchIters = 1 << 20

type nibbleSeedKey struct{ nibble, seed int }

var (
	nibbleAddressCacheMu sync.Mutex
	nibbleAddressCache   = make(map[nibbleSeedKey][]byte)
)

func findAddressForNibble(targetNibble int, seed int) []byte {
	if targetNibble < 0 || targetNibble > 0xf {
		panic(fmt.Sprintf("findAddressForNibble: nibble %d out of range [0,15]", targetNibble))
	}
	key := nibbleSeedKey{targetNibble, seed}

	nibbleAddressCacheMu.Lock()
	if cached, ok := nibbleAddressCache[key]; ok {
		nibbleAddressCacheMu.Unlock()
		return append([]byte(nil), cached...) // copy so callers can't mutate the shared cache
	}
	nibbleAddressCacheMu.Unlock()

	var addr [20]byte
	counter := uint64(seed) * 1_000_003
	for range maxAddrSearchIters {
		binary.BigEndian.PutUint64(addr[:8], counter)
		h := crypto.Keccak256(addr[:])
		if int(h[0]>>4) == targetNibble {
			result := make([]byte, 20)
			copy(result, addr[:])

			nibbleAddressCacheMu.Lock()
			nibbleAddressCache[key] = result
			nibbleAddressCacheMu.Unlock()
			return append([]byte(nil), result...)
		}
		counter++
	}
	panic(fmt.Sprintf("findAddressForNibble(nibble=%d, seed=%d): exceeded %d iterations", targetNibble, seed, maxAddrSearchIters))
}

func findAddressForHexPrefix(nibblePrefix []byte, seed int) []byte {
	for i, n := range nibblePrefix {
		if n > 0xf {
			panic(fmt.Sprintf("findAddressForHexPrefix: nibble %d at %d out of range [0,15]", n, i))
		}
	}
	var addr [20]byte
	counter := uint64(seed)*2_654_435_761 + 1
	for range maxAddrSearchIters {
		binary.BigEndian.PutUint64(addr[:8], counter)
		h := crypto.Keccak256(addr[:])
		match := true
		for i, n := range nibblePrefix {
			var hn byte
			if i%2 == 0 {
				hn = h[i/2] >> 4
			} else {
				hn = h[i/2] & 0xf
			}
			if hn != n {
				match = false
				break
			}
		}
		if match {
			result := make([]byte, 20)
			copy(result, addr[:])
			return result
		}
		counter++
	}
	panic(fmt.Sprintf("findAddressForHexPrefix(%v, seed=%d): exceeded %d iterations", nibblePrefix, seed, maxAddrSearchIters))
}

func mockTrieCtxFactory(ms *MockState) commitment.TrieContextFactory {
	return func(context.Context) (commitment.PatriciaContext, func()) {
		return ms, func() {}
	}
}

func addrHex(addr []byte) string {
	return hex.EncodeToString(addr)
}
