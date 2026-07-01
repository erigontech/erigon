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
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

func TestDeepIntegration_BranchParity(t *testing.T) {
	pk, upds := buildWhaleCorpus(bigAccountWhale(15_000))
	ctx := context.Background()

	seqMs := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	seqRoot := processBatch(t, seqMs, seq, pk, upds)

	for _, workers := range benchWorkerCounts() {
		parMs := NewMockState(t)
		parMs.SetConcurrentCommitment(true)
		require.NoError(t, parMs.applyPlainUpdates(pk, upds))
		pph := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
		pph.SetNumWorkers(workers)
		pph.ResetContext(parMs)
		pUpd := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, pk, upds)
		parRoot, err := pph.Process(ctx, pUpd, "", nil, WarmupConfig{})
		require.NoError(t, err)
		pUpd.Close()
		pph.Release()

		require.Equalf(t, seqRoot, parRoot, "deep parallel(workers=%d) root != sequential", workers)

		requireBranchParity(t, seqMs, parMs)
	}
}

type storKV struct {
	hk  []byte
	pk  []byte
	upd Update
}

func whaleByNibble(slots int) (addr []byte, accHash []byte, accNib int, accUpd Update, pk [][]byte, upds []Update, groups [16][]storKV) {
	rnd := rand.New(rand.NewSource(424242))
	addr = make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 12345)
	for i := 0; i < slots; i++ {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		val := make([]byte, 32)
		rnd.Read(val)
		ub.Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
	}
	pk, upds = ub.Build()
	accHash = KeyToHexNibbleHash(addr)
	accNib = int(accHash[63])
	for i, k := range pk {
		if len(k) == length.Addr {
			accUpd = upds[i]
			continue
		}
		h := KeyToHexNibbleHash(k)
		x := int(h[64])
		groups[x] = append(groups[x], storKV{hk: h, pk: k, upd: upds[i]})
	}
	return addr, accHash, accNib, accUpd, pk, upds, groups
}

func foldChildAt(w *HexPatriciaHashed, accNib int, g []storKV) (cell, error) {
	for i := range g {
		if err := w.followAndUpdate(g[i].hk, g[i].pk, &g[i].upd); err != nil {
			return cell{}, err
		}
	}
	for w.activeRows > 1 {
		if err := w.fold(); err != nil {
			return cell{}, err
		}
	}
	c := w.grid[0][accNib]
	if c.hashedExtLen > 0 {
		c.hashedExtLen--
		copy(c.hashedExtension[:], c.hashedExtension[1:])
	}
	if c.extLen > 0 {
		c.extLen--
		copy(c.extension[:], c.extension[1:])
	}
	return c, nil
}

func concurrentAccountRoot(ms *MockState, addr, accHash []byte, accNib int, accUpd Update, groups [16][]storKV, parallel bool) ([]byte, error) {
	var children [16]cell
	var present uint16
	run := func(x int) error {
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		c, err := foldChildAt(w, accNib, groups[x])
		w.Release()
		if err != nil {
			return err
		}
		children[x] = c
		return nil
	}
	if parallel {
		var eg errgroup.Group
		for x := 0; x < 16; x++ {
			if len(groups[x]) == 0 {
				continue
			}
			present |= uint16(1) << x
			x := x
			eg.Go(func() error { return run(x) })
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
	} else {
		for x := 0; x < 16; x++ {
			if len(groups[x]) == 0 {
				continue
			}
			present |= uint16(1) << x
			if err := run(x); err != nil {
				return nil, err
			}
		}
	}

	asm := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer asm.Release()
	copy(asm.currentKey[:], accHash[:64])
	asm.currentKeyLen = 64
	asm.depths[0] = 64
	asm.depths[1] = 65
	asm.activeRows = 2
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = empty.CodeHash
	ac.setFromUpdate(&accUpd)
	asm.grid[0][accNib] = ac
	asm.touchMap[0] = uint16(1) << accNib
	asm.afterMap[0] = uint16(1) << accNib
	for x := 0; x < 16; x++ {
		if present&(uint16(1)<<x) != 0 {
			asm.grid[1][x] = children[x]
		}
	}
	asm.touchMap[1] = present
	asm.afterMap[1] = present
	for asm.activeRows > 0 {
		if err := asm.fold(); err != nil {
			return nil, err
		}
	}
	return asm.RootHash()
}

func TestDeepConcurrent_WhaleParity(t *testing.T) {
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(750_000)

	ms := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	seqRoot := processBatch(t, ms, seq, pk, upds)

	conRoot, err := concurrentAccountRoot(ms, addr, accHash, accNib, accUpd, groups, true)
	require.NoError(t, err)
	require.Equal(t, seqRoot, conRoot, "concurrent storage-fold root != sequential")
}
