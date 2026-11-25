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

package v2

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func additiveCorpus() (keys [][]byte, partials [][2]*commitment.Update, merged []commitment.Update) {
	accA := []byte("\x4c\x88\x85\x35\x84\x1a\xcb\xe0\x70\x9b\x07\x58\x08\x3f\x61\xd3\x75\xbc\x02\xb4")
	accB := []byte("\x68\xee\x6c\x0e\x9c\xdc\x73\xb2\xb2\xd5\x2d\xbd\x79\xf1\x9d\x24\xfe\x25\xe2\xf9")
	slot := append(append([]byte{}, accA...), make([]byte, 31)...)
	slot = append(slot, 0x01)

	keys = [][]byte{accA, accB, slot}

	balA := &commitment.Update{Flags: commitment.BalanceUpdate, Balance: *uint256.NewInt(1000)}
	nonceA := &commitment.Update{Flags: commitment.NonceUpdate, Nonce: 7}
	delB := &commitment.Update{Flags: commitment.DeleteUpdate}
	balB := &commitment.Update{Flags: commitment.BalanceUpdate, Balance: *uint256.NewInt(500)}
	stale := &commitment.Update{Flags: commitment.StorageUpdate}
	stale.StorageLen = 1
	stale.Storage[0] = 42
	final := &commitment.Update{Flags: commitment.StorageUpdate}
	final.StorageLen = 1
	final.Storage[0] = 7

	partials = [][2]*commitment.Update{{balA, nonceA}, {delB, balB}, {stale, final}}

	merged = []commitment.Update{
		{Flags: commitment.BalanceUpdate | commitment.NonceUpdate, Balance: *uint256.NewInt(1000), Nonce: 7},
		{Flags: commitment.BalanceUpdate, Balance: *uint256.NewInt(500)},
		*final,
	}
	return keys, partials, merged
}

func TestAdditiveTouch(t *testing.T) {
	t.Parallel()

	t.Run("parallel", func(t *testing.T) {
		keys, partials, merged := additiveCorpus()
		seqRoot, _ := sequentialRoot(t, keys, merged)

		parMs := NewMockState(t)
		parMs.SetConcurrentCommitment(true)
		require.NoError(t, parMs.applyPlainUpdates(keys, merged))

		parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, commitment.DefaultTrieConfig())
		defer parTrie.Release()
		parTrie.SetNumWorkers(2)
		parTrie.ResetContext(parMs)

		ut := commitment.NewUpdates(commitment.ModeParallel, t.TempDir(), commitment.KeyToHexNibbleHash)
		defer ut.Close()
		for i, k := range keys {
			ut.TouchPlainKeyDirect(string(k), partials[i][0])
			ut.TouchPlainKeyDirect(string(k), partials[i][1])
		}
		require.Equal(t, uint64(len(keys)), ut.Size(), "re-touches must not add keys")

		parRoot, err := parTrie.Process(context.Background(), ut, "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, seqRoot, parRoot, "additive partial touches must fold to the merged root")
	})
}
