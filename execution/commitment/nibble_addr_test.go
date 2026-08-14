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

package commitment

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/crypto"
)

// maxAddrSearchIters bounds brute-force search so a broken search space fails loudly instead of hanging.
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

func mockTrieCtxFactory(ms *MockState) TrieContextFactory {
	return func(context.Context) (PatriciaContext, func()) {
		return ms, func() {}
	}
}

func addrHex(addr []byte) string {
	return hex.EncodeToString(addr)
}
