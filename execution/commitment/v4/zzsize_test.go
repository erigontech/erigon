// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
// more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/rlp"
)

func sizeEOA(i int, rnd *rand.Rand) (*commitment.Update, []byte) {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate}
	u.Nonce = uint64(rnd.Intn(500))
	u.Balance = *uint256.NewInt(uint64(rnd.Int63n(4e18)))
	u.CodeHash = empty.CodeHash
	return u, nil
}

func sizeContract(i int, rnd *rand.Rand) (*commitment.Update, []byte) {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
	u.Nonce = 1
	u.Balance = *uint256.NewInt(uint64(rnd.Int63n(1e15)))
	u.CodeHash = common.HexToHash(fmt.Sprintf("0x%064x", i+7))
	root := make([]byte, 32)
	rnd.Read(root)
	return u, root
}

func doubleRLPLen(v []byte) int {
	var buf bytes.Buffer
	var pfx [8]byte
	if err := (rlp.RlpSerializableBytes(v)).ToDoubleRLP(&buf, pfx[:]); err != nil {
		panic(err)
	}
	return buf.Len()
}

func TestZZLeafEncodingSize(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	const n = 200000
	const contractShare = 12

	var packed, consensus int
	var eoaN, ctrN int
	for i := range n {
		var u *commitment.Update
		var root []byte
		if i%100 < contractShare {
			u, root = sizeContract(i, rnd)
			ctrN++
		} else {
			u, root = sizeEOA(i, rnd)
			eoaN++
		}
		p := encodeAccountLeaf(u, root, nil)
		nonce, bal, ch, sr, err := decodeAccountLeaf(p)
		if err != nil {
			t.Fatal(err)
		}
		c := accountConsensusRLP(nonce, &bal, sr, ch, nil)
		packed += len(p)
		consensus += len(c)
	}
	t.Logf("accounts n=%d (EOA %d / contract %d)", n, eoaN, ctrN)
	t.Logf("  packed    %9d B  avg %6.2f B/leaf", packed, float64(packed)/n)
	t.Logf("  consensus %9d B  avg %6.2f B/leaf", consensus, float64(consensus)/n)
	t.Logf("  delta     %9d B  (%+.1f%%)  %+.2f B/leaf", consensus-packed,
		100*float64(consensus-packed)/float64(packed), float64(consensus-packed)/n)

	var raw, drlp int
	for range n {
		l := 1 + rnd.Intn(32)
		if rnd.Intn(100) < 55 {
			l = 1 + rnd.Intn(4)
		}
		v := make([]byte, l)
		rnd.Read(v)
		v[0] |= 0x01
		raw += l
		drlp += doubleRLPLen(v)
	}
	t.Logf("storage n=%d", n)
	t.Logf("  raw       %9d B  avg %6.2f B/leaf", raw, float64(raw)/n)
	t.Logf("  doubleRLP %9d B  avg %6.2f B/leaf", drlp, float64(drlp)/n)
	t.Logf("  delta     %9d B  (%+.1f%%)  %+.2f B/leaf", drlp-raw,
		100*float64(drlp-raw)/float64(raw), float64(drlp-raw)/n)
}

func BenchmarkZZAccountLeafCodec(b *testing.B) {
	rnd := rand.New(rand.NewSource(7))
	u, _ := sizeEOA(1, rnd)
	packed := encodeAccountLeaf(u, nil, nil)
	buf := make([]byte, 0, 128)

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			buf = encodeAccountLeaf(u, nil, buf[:0])
		}
	})
	b.Run("decode+consensusRLP", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			nonce, bal, ch, sr, err := decodeAccountLeaf(packed)
			if err != nil {
				b.Fatal(err)
			}
			buf = accountConsensusRLP(nonce, &bal, sr, ch, buf[:0])
		}
	})
	b.Run("consensusRLP_only", func(b *testing.B) {
		b.ReportAllocs()
		bal := u.Balance
		for range b.N {
			buf = accountConsensusRLP(u.Nonce, &bal, nil, nil, buf[:0])
		}
	})
}

func BenchmarkZZStorageLeafRef(b *testing.B) {
	v := make([]byte, 32)
	rand.New(rand.NewSource(3)).Read(v)
	suffix := make([]byte, 33)
	buf := make([]byte, 0, 256)
	b.Run("storageLeafRef", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = storageLeafRef(suffix, v, buf[:0])
		}
	})
	b.Run("leafRef_account", func(b *testing.B) {
		payload := accountConsensusRLP(3, uint256.NewInt(12345), nil, nil, nil)
		b.ReportAllocs()
		for range b.N {
			_ = leafRef(planeAccount, suffix, payload, buf[:0])
		}
	})
}

func storageLeafRefBuffered(suffix []byte, payload []byte, dst []byte) []byte {
	var encoded bytes.Buffer
	var prefix [8]byte
	if err := (rlp.RlpSerializableBytes(payload)).ToDoubleRLP(&encoded, prefix[:]); err != nil {
		panic(err)
	}
	contentLen := rlp.StringLen(suffix) + encoded.Len()
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	pos += copy(dst[pos:], encoded.Bytes())
	encodedBytes := dst[start:pos]
	if len(encodedBytes) < 32 {
		return encodedBytes
	}
	hash := keccak.Sum256(encodedBytes)
	return append(dst[:start], hash[:]...)
}

func storageLeafRefDirect(suffix []byte, payload []byte, dst []byte) []byte {
	innerLen := 1 + len(payload)
	if len(payload) == 1 && payload[0] < 0x80 {
		innerLen = 1
	}
	outerLen := 1 + innerLen
	if innerLen == 1 && payload[0] < 0x80 {
		outerLen = 1
	}
	contentLen := rlp.StringLen(suffix) + outerLen
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen)+contentLen)...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	if outerLen == 1 {
		dst[pos] = payload[0]
		pos++
	} else {
		if innerLen == 1 {
			dst[pos] = 0x81
			dst[pos+1] = payload[0]
			pos += 2
		} else {
			dst[pos] = byte(0x80 + outerLen - 1)
			dst[pos+1] = byte(0x80 + len(payload))
			pos += 2
			pos += copy(dst[pos:], payload)
		}
	}
	out := dst[start:pos]
	if len(out) < 32 {
		return out
	}
	h := keccak.Sum256(out)
	return append(dst[:start], h[:]...)
}

func TestZZStorageLeafRefDirectEquivalence(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	for _, suffixLen := range []int{1, 2, 17, 33} {
		suffix := make([]byte, suffixLen)
		for l := 1; l <= 32; l++ {
			for trial := range 400 {
				v := make([]byte, l)
				rnd.Read(v)
				if trial < 256 && l == 1 {
					v[0] = byte(trial)
				}
				if v[0] == 0 && l > 1 {
					v[0] = 1
				}
				rnd.Read(suffix)
				want := storageLeafRefBuffered(suffix, v, nil)
				got := storageLeafRefDirect(suffix, v, nil)
				if !bytes.Equal(want, got) {
					t.Fatalf("suffixLen=%d l=%d v=%x\n want %x\n got  %x", suffixLen, l, v, want, got)
				}
			}
		}
	}
}

func BenchmarkZZStorageLeafRefDirect(b *testing.B) {
	v := make([]byte, 32)
	rand.New(rand.NewSource(3)).Read(v)
	suffix := make([]byte, 33)
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for range b.N {
		_ = storageLeafRefDirect(suffix, v, buf[:0])
	}
}

func benchUpdates2(t *testing.T, entries []parityUpdate) *commitment.Updates {
	u := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	for _, e := range entries {
		u.TouchPlainKeyDirect(string(e.key), e.update)
	}
	return u
}

func commitmentWarmup() commitment.WarmupConfig { return commitment.WarmupConfig{} }

func TestZZWorkerSweep(t *testing.T) {
	ctxb := context.Background()
	for _, shape := range []string{"storage", "accounts"} {
		entries := benchEntries(shape, 100000)
		for _, w := range []int{1, 2, 4, 8, 18, 36} {
			var best time.Duration
			for iter := range 3 {
				c := newParityContext()
				tr := &Trie{scheduleWorkers: w}
				tr.ResetContext(c)
				u := benchUpdates2(t, entries)
				start := time.Now()
				if _, err := tr.Process(ctxb, u, "", nil, commitmentWarmup()); err != nil {
					t.Fatal(err)
				}
				d := time.Since(start)
				if iter == 0 || d < best {
					best = d
				}
				tr.Release()
			}
			fmt.Printf("%-9s workers=%-3d %8.1f ms\n", shape, w, float64(best.Microseconds())/1000)
		}
	}
}
