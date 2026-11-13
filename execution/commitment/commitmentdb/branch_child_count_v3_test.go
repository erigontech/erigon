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

package commitmentdb

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

var _ v3Trie = (*v3.Trie)(nil)

type recordsStateReader struct {
	testStateReader
	records map[string][]byte
}

func (r *recordsStateReader) Read(_ kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	return r.records[string(key)], 0, nil
}

func TestBranchChildCountUnderCommitmentV3(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1))
	ops := make([]commitmenttest.Op, 0, 64)
	for range 64 {
		key := make([]byte, length.Addr)
		rng.Read(key)
		account := commitmenttest.RandomAccount(rng)
		ops = append(ops, commitmenttest.Op{Key: key, Account: &account})
	}
	process := func(open func(commitment.PatriciaContext) commitment.Trie, mode commitment.Mode) map[string][]byte {
		mem := runner.NewMemory(runner.ContextSpec{})
		mem.Apply(ops)
		pc, _ := mem.Open(ctx)
		tr := open(pc)
		updates := commitment.NewUpdates(mode, t.TempDir(), commitment.KeyToHexNibbleHash)
		defer updates.Close()
		for _, op := range ops {
			updates.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
		}
		_, err := tr.Process(ctx, updates, "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		return mem.Records()
	}
	hphRecords := process(func(pc commitment.PatriciaContext) commitment.Trie {
		return commitment.NewHexPatriciaHashed(length.Addr, pc, commitment.DefaultTrieConfig())
	}, commitment.ModeUpdate)
	v3Trie, v3Updates := v3.NewTrie(t.TempDir(), commitment.DefaultTrieConfig())
	v3Updates.Close()
	v3Records := process(func(pc commitment.PatriciaContext) commitment.Trie {
		v3Trie.ResetContext(pc)
		return v3Trie
	}, commitment.ModeCollect)
	sdc := &SharedDomainsCommitmentContext{
		sharedDomains: &branchChildCountDomains{},
		stateReader:   &recordsStateReader{records: v3Records},
		patriciaTrie:  v3Trie,
	}

	branches := 0
	for _, op := range ops {
		full := commitment.KeyToHexNibbleHash(op.Key)
		for cut := 0; cut <= len(full); cut++ {
			want := commitment.BranchData(hphRecords[string(nibbles.HexToCompact(full[:cut]))]).ChildCount()
			if want >= 2 {
				branches++
			}
			got, err := sdc.BranchChildCount(full[:cut])
			require.NoError(t, err)
			require.Equal(t, want, got, "prefix %x", full[:cut])
		}
	}
	require.Positive(t, branches)
}
