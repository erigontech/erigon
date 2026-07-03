// Copyright 2024 The Erigon Authors
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// benchCapturedSuperset builds a trie of accts accounts each with slots storage
// slots, then captures the witness superset (produceExclusionProofs) for the first
// `touch` accounts, returning the captured nodes, the proved (fold) keys, and root.
func benchCapturedSuperset(b *testing.B, accts, slots, touch int) (full, provedKeys [][]byte, root []byte) {
	b.Helper()
	ctx := context.Background()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	builder := NewUpdateBuilder()
	addrs := make([][]byte, 0, accts)
	for i := 0; i < accts; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		addrs = append(addrs, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		for j := 0; j < slots; j++ {
			slot := common.FromHex(fmt.Sprintf("%064x", j+1))
			builder.Storage(common.Bytes2Hex(a), common.Bytes2Hex(slot), common.Bytes2Hex(slot))
		}
	}
	plainKeys, updates := builder.Build()
	require.NoError(b, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	_, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(b, err)

	toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	defer toWitness.Close()
	for _, a := range addrs[:touch] {
		toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
		for j := 0; j < slots; j++ {
			slot := common.FromHex(fmt.Sprintf("%064x", j+1))
			toWitness.TouchPlainKey(string(storageKey(a, slot)), nil, toWitness.TouchStorage)
		}
	}
	full, provedKeys, root, err = hph.Witnesses(ctx, toWitness, true, "")
	require.NoError(b, err)
	return full, provedKeys, root
}

// BenchmarkWitnessPrune_RLPDecode measures the current prune: decode the whole
// captured superset into a trie, then walk the proof paths (re-hashing each node).
func BenchmarkWitnessPrune_RLPDecode(b *testing.B) {
	full, provedKeys, _ := benchCapturedSuperset(b, 512, 8, 32)
	b.Logf("captured superset nodes=%d provedKeys=%d", len(full), len(provedKeys))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wt, err := trie.RLPDecode(full)
		if err != nil {
			b.Fatal(err)
		}
		lean, err := wt.WitnessNodesForKeys(provedKeys)
		if err != nil {
			b.Fatal(err)
		}
		if len(lean) == 0 {
			b.Fatal("empty lean set")
		}
	}
}

// BenchmarkBranchWitnessTotal measures the branch's full on-the-fly witness build —
// capture (Witnesses) + prune (RLPDecode + WitnessNodesForKeys) — on the same fixture
// as main's BenchmarkMainGenerateWitness, for a fresh main-vs-branch comparison.
func BenchmarkBranchWitnessTotal(b *testing.B) {
	ctx := context.Background()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	builder := NewUpdateBuilder()
	addrs := make([][]byte, 0, 512)
	for i := 0; i < 512; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		addrs = append(addrs, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		for j := 0; j < 8; j++ {
			slot := common.FromHex(fmt.Sprintf("%064x", j+1))
			builder.Storage(common.Bytes2Hex(a), common.Bytes2Hex(slot), common.Bytes2Hex(slot))
		}
	}
	plainKeys, updates := builder.Build()
	require.NoError(b, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	_, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
		for _, a := range addrs[:32] {
			toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
			for j := 0; j < 8; j++ {
				slot := common.FromHex(fmt.Sprintf("%064x", j+1))
				toWitness.TouchPlainKey(string(storageKey(a, slot)), nil, toWitness.TouchStorage)
			}
		}
		full, provedKeys, _, err := hph.Witnesses(ctx, toWitness, true, "")
		toWitness.Close()
		require.NoError(b, err)
		if _, err := trie.WitnessNodesForKeysFromNodes(full, provedKeys); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWitnessPrune_ByHash measures the byHash-walk prune: index the captured
// nodes by hash and decode only the proof-path nodes, emitting cached bytes.
func BenchmarkWitnessPrune_ByHash(b *testing.B) {
	full, provedKeys, _ := benchCapturedSuperset(b, 512, 8, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lean, err := trie.WitnessNodesForKeysFromNodes(full, provedKeys)
		if err != nil {
			b.Fatal(err)
		}
		if len(lean) == 0 {
			b.Fatal("empty lean set")
		}
	}
}

// BenchmarkWitnessCapture measures the fold-time capture (Witnesses) for the same
// workload, so the prune's share of total witness cost is visible.
func BenchmarkWitnessCapture(b *testing.B) {
	ctx := context.Background()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	builder := NewUpdateBuilder()
	addrs := make([][]byte, 0, 512)
	for i := 0; i < 512; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		addrs = append(addrs, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		for j := 0; j < 8; j++ {
			slot := common.FromHex(fmt.Sprintf("%064x", j+1))
			builder.Storage(common.Bytes2Hex(a), common.Bytes2Hex(slot), common.Bytes2Hex(slot))
		}
	}
	plainKeys, updates := builder.Build()
	require.NoError(b, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	_, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
		for _, a := range addrs[:32] {
			toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
		}
		_, _, _, err := hph.Witnesses(ctx, toWitness, true, "")
		toWitness.Close()
		require.NoError(b, err)
	}
}
