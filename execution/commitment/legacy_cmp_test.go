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
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"
)

func buildLegacyTrie(pk [][]byte, upds []Update) *trie.Trie {
	tr := trie.New(trie.EmptyRoot)
	for i, k := range pk {
		if len(k) != length.Addr {
			continue
		}
		u := &upds[i]
		codeHash := u.CodeHash
		if codeHash == (common.Hash{}) {
			codeHash = empty.CodeHash
		}
		var acc accounts.Account
		acc.Nonce = u.Nonce
		acc.Balance = u.Balance
		acc.CodeHash = accounts.InternCodeHash(codeHash)
		acc.Root = trie.EmptyRoot
		h := keccak.Sum256(k)
		tr.UpdateAccount(h[:], &acc)
	}
	for i, k := range pk {
		if len(k) == length.Addr {
			continue
		}
		u := &upds[i]
		ah := keccak.Sum256(k[:length.Addr])
		sh := keccak.Sum256(k[length.Addr:])
		tr.Update(dbutils.GenerateCompositeTrieKey(ah, sh), u.Storage[:u.StorageLen])
	}
	return tr
}

func TestLegacyVsHexRoot(t *testing.T) {
	for _, c := range []struct {
		name string
		opts whaleOpts
	}{
		{"tiny", whaleOpts{seed: 7, smallBefore: 3, smallBeforeSlots: 2, bigSlots: 5, tailAccounts: 2}},
		{"accountsOnly", whaleOpts{seed: 11, tailAccounts: 5000}},
		{"whale50K", bigAccountWhale(50_000)},
		{"mixed", whaleOpts{seed: 99, smallBefore: 200, smallBeforeSlots: 7, bigSlots: 20_000, extraWhales: []int{3_000}, smallAfter: 200, smallAfterSlots: 3, tailAccounts: 5_000}},
	} {
		t.Run(c.name, func(t *testing.T) {
			pk, upds := buildWhaleCorpus(c.opts)

			ms := NewMockState(t)
			require.NoError(t, ms.applyPlainUpdates(pk, upds))
			hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			u := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
			hexRoot, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
			require.NoError(t, err)
			u.Close()

			require.Equal(t, common.BytesToHash(hexRoot), buildLegacyTrie(pk, upds).Hash(),
				"keys=%d", len(pk))
		})
	}
}
