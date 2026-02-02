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

package changeset_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

func TestSerializeDeserializeDiff(t *testing.T) {
	t.Parallel()

	var d []kv.DomainEntryDiff
	step1, step2, step3 := [8]byte{1}, [8]byte{2}, [8]byte{3}
	d = append(d, kv.DomainEntryDiff{Key: "key188888888", Value: []byte("value1"), PrevStepBytes: step1[:]})
	d = append(d, kv.DomainEntryDiff{Key: "key288888888", Value: []byte("value2"), PrevStepBytes: step2[:]})
	d = append(d, kv.DomainEntryDiff{Key: "key388888888", Value: []byte("value3"), PrevStepBytes: step3[:]})
	d = append(d, kv.DomainEntryDiff{Key: "key388888888", Value: []byte("value3"), PrevStepBytes: step1[:]})

	serialized := changeset.SerializeDiffSet(d, nil)
	fmt.Println(len(serialized))
	deserialized := changeset.DeserializeDiffSet(serialized)

	require.Equal(t, d, deserialized)
}

func TestSerializeDeserializeDiffEmpty(t *testing.T) {
	t.Parallel()

	var empty []kv.DomainEntryDiff
	serialized := changeset.SerializeDiffSet(empty, nil)
	require.Equal(t, []byte{0, 0, 0, 0, 0}, serialized) // dict len (1) + diffSet len (4)
	deserialized := changeset.DeserializeDiffSet(serialized)
	require.Empty(t, deserialized)
}

func TestMergeDiffSet(t *testing.T) {
	t.Parallel()

	var d1 []kv.DomainEntryDiff
	step1, step2, step3 := [8]byte{1}, [8]byte{2}, [8]byte{3}
	d1 = append(d1, kv.DomainEntryDiff{Key: "key188888888", Value: []byte("value1"), PrevStepBytes: step1[:]})
	d1 = append(d1, kv.DomainEntryDiff{Key: "key288888888", Value: []byte("value2"), PrevStepBytes: step2[:]})
	d1 = append(d1, kv.DomainEntryDiff{Key: "key388888888", Value: []byte("value3"), PrevStepBytes: step3[:]})

	var d2 []kv.DomainEntryDiff
	step4, step5, step6 := [8]byte{4}, [8]byte{5}, [8]byte{6}
	d2 = append(d2, kv.DomainEntryDiff{Key: "key188888888", Value: []byte("value5"), PrevStepBytes: step5[:]})
	d2 = append(d2, kv.DomainEntryDiff{Key: "key388888888", Value: []byte("value6"), PrevStepBytes: step6[:]})
	d2 = append(d2, kv.DomainEntryDiff{Key: "key488888888", Value: []byte("value4"), PrevStepBytes: step4[:]})

	merged := changeset.MergeDiffSets(d1, d2)
	require.Len(t, merged, 4)

	require.Equal(t, d2[0], merged[0])
	require.Equal(t, d1[1], merged[1])
	require.Equal(t, d2[1], merged[2])
	require.Equal(t, d2[2], merged[3])
}

func BenchmarkSerializeDiffSet(b *testing.B) {
	// Create a realistic diffSet with varying sizes
	var d []kv.DomainEntryDiff
	steps := [][8]byte{{1}, {2}, {3}, {4}}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d_padding", i)
		value := make([]byte, 32+i%64) // varying value sizes
		d = append(d, kv.DomainEntryDiff{
			Key:           key,
			Value:         value,
			PrevStepBytes: steps[i%len(steps)][:],
		})
	}

	out := make([]byte, 0, 128*1024)
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		out = changeset.SerializeDiffSet(d, out[:0])
	}
}

// createTestDiffSet creates a StateChangeSet with realistic data
func createTestDiffSet(tb testing.TB, numAccounts, numStorage, numCode, numCommitment int) *changeset.StateChangeSet {
	tb.Helper()

	diffSet := &changeset.StateChangeSet{}

	// Accounts domain - 20 byte addresses with account data
	for i := 0; i < numAccounts; i++ {
		key := make([]byte, 20)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 70) // typical account encoding size
		diffSet.Diffs[kv.AccountsDomain].DomainUpdate(key, kv.Step(100), value, kv.Step(99))
	}

	// Storage domain - 20 byte address + 32 byte location
	for i := 0; i < numStorage; i++ {
		key := make([]byte, 52)
		key[0] = byte(i >> 16)
		key[1] = byte(i >> 8)
		key[2] = byte(i)
		value := make([]byte, 32) // storage value
		diffSet.Diffs[kv.StorageDomain].DomainUpdate(key, kv.Step(100), value, kv.Step(99))
	}

	// Code domain - 20 byte address with code hash
	for i := 0; i < numCode; i++ {
		key := make([]byte, 20)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 32) // code hash
		diffSet.Diffs[kv.CodeDomain].DomainUpdate(key, kv.Step(100), value, kv.Step(99))
	}

	// Commitment domain - variable key with trie node data
	for i := 0; i < numCommitment; i++ {
		key := make([]byte, 8+i%32) // variable length keys
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 64+i%64) // variable length values
		diffSet.Diffs[kv.CommitmentDomain].DomainUpdate(key, kv.Step(100), value, kv.Step(99))
	}

	return diffSet
}
