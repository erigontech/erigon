// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// Shared helpers for commitment tests: brute-force address generation keyed by
// hashed-key nibble and a mock trie-context factory.

package commitment

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/crypto"
)

// maxAddrSearchIters bounds the brute-force address search helpers below so a
// broken search space (e.g. hash function change) produces a descriptive panic
// instead of an infinite hang. 1M iterations is well above the expected work:
// a single-nibble hit averages ~16 iters; a 16-bit shared-prefix hit averages
// ~65k, both comfortably under the cap.
const maxAddrSearchIters = 1 << 20

// nibbleSeedKey is the composite cache key for findAddressForNibble.
type nibbleSeedKey struct{ nibble, seed int }

// nibbleAddressCache caches brute-forced addresses keyed by (nibble, seed) to
// avoid repeated keccak work across tests and ensure each seed always returns
// the same deterministic address regardless of call order.
var (
	nibbleAddressCacheMu sync.Mutex
	nibbleAddressCache   = make(map[nibbleSeedKey][]byte)
)

// findAddressForNibble brute-force searches for a 20-byte address whose
// keccak256 first nibble (upper 4 bits of hash[0]) matches targetNibble.
// seed controls the starting point for the search; each unique seed produces
// a different address. Results are cached globally.
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

	// Brute force: we encode a counter into the first 8 bytes of a 20-byte
	// address and increment until keccak(addr)[0] >> 4 == targetNibble.
	var addr [20]byte
	// Use seed * large prime to separate search spaces for different seeds.
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

// findAddressForHexPrefix brute-force searches for a 20-byte address whose keccak256
// hashed-key nibbles start with the given nibble prefix (each entry in [0,15]). seed
// separates search spaces. Used to force accounts to share a multi-nibble hashed prefix
// (e.g. an extension-topped subtree under one root nibble).
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

// mockTrieCtxFactory returns a TrieContextFactory that always returns the
// given MockState and a no-op cleanup.
func mockTrieCtxFactory(ms *MockState) TrieContextFactory {
	return func() (PatriciaContext, func()) {
		return ms, func() {}
	}
}

// addrHex returns the hex-encoded string of a 20-byte address (no 0x prefix),
// suitable for passing to UpdateBuilder methods.
func addrHex(addr []byte) string {
	return hex.EncodeToString(addr)
}
