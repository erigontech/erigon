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
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/commitment/trie"
	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"
)

func buildDelta(pk [][]byte, upds []Update, n int, seed int64) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	idx := make([]int, 0, len(pk))
	for i, k := range pk {
		if len(k) != length.Addr {
			idx = append(idx, i)
		}
	}
	rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })
	n = min(n, len(idx))
	dk := make([][]byte, 0, n)
	du := make([]Update, 0, n)
	for _, i := range idx[:n] {
		u := upds[i]
		var v [32]byte
		rnd.Read(v[:])
		u.Storage = common.Hash(v)
		u.StorageLen = 32
		u.Flags = StorageUpdate
		dk = append(dk, pk[i])
		du = append(du, u)
	}
	return dk, du
}

func applyDeltaLegacy(tr *trie.Trie, dk [][]byte, du []Update) common.Hash {
	for i, k := range dk {
		u := &du[i]
		ah := keccak.Sum256(k[:length.Addr])
		sh := keccak.Sum256(k[length.Addr:])
		tr.Update(dbutils.GenerateCompositeTrieKey(ah, sh), u.Storage[:u.StorageLen])
	}
	return tr.Hash()
}

func TestIncrementalRootsAgree(t *testing.T) {
	for _, warmup := range []struct {
		name    string
		enabled bool
		opts    whaleOpts
	}{
		{name: "off", opts: bigAccountWhale(50_000)},
		{name: "on", enabled: true, opts: bigAccountWhale(50_000)},
		{name: "E83/100K", opts: bigAccountWhale(100_000)},
		{name: "E83/1M", opts: whale1M()},
	} {
		t.Run(warmup.name, func(t *testing.T) {
			pk, upds := buildWhaleCorpus(warmup.opts)
			dk, du := buildDelta(pk, upds, 500, 4242)

			ms := NewMockState(t)
			require.NoError(t, ms.applyPlainUpdates(pk, upds))
			hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
			warmupConfig := WarmupConfig{}
			if warmup.enabled {
				ms.SetConcurrentCommitment(true)
				warmupConfig = WarmupConfig{
					Enabled:    true,
					CtxFactory: mockTrieCtxFactory(ms),
					NumWorkers: 1,
					MaxDepth:   WarmupMaxDepth,
				}
			}
			_, err := hph.Process(context.Background(), u1, "", nil, warmupConfig)
			require.NoError(t, err)
			u1.Close()

			require.NoError(t, ms.applyPlainUpdates(dk, du))
			u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, dk, du)
			hexRoot, err := hph.Process(context.Background(), u2, "", nil, warmupConfig)
			require.NoError(t, err)
			u2.Close()

			tr := buildLegacyTrie(pk, upds)
			tr.Hash()
			require.Equal(t, common.BytesToHash(hexRoot), applyDeltaLegacy(tr, dk, du), "delta keys=%d", len(dk))
		})
	}
}
