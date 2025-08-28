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

package state

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func TestOverflowPages(t *testing.T) {
	db, _ := testDbAndAggregatorv3(t, 10)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	k, v := make([]byte, diffChunkKeyLen), make([]byte, diffChunkLen)
	k[0] = 0
	_ = tx.Put(kv.ChangeSets3, k, v)
	k[0] = 1
	_ = tx.Put(kv.ChangeSets3, k, v)
	st, err := tx.(*mdbx.MdbxTx).BucketStat(kv.ChangeSets3)
	require.NoError(t, err)
	require.Equal(t, 2, int(st.OverflowPages))
	require.Equal(t, 1, int(st.LeafPages))
	require.Equal(t, 2, int(st.Entries))
	require.Equal(t, 2, int(st.Entries))
}

func TestSerializeDeserializeDiff(t *testing.T) {
	t.Parallel()

	var d []kv.DomainEntryDiff
	step1, step2, step3 := [8]byte{1}, [8]byte{2}, [8]byte{3}
	d = append(d, kv.DomainEntryDiff{Key: "key188888888", Value: []byte("value1"), PrevStepBytes: step1[:]})
	d = append(d, kv.DomainEntryDiff{Key: "key288888888", Value: []byte("value2"), PrevStepBytes: step2[:]})
	d = append(d, kv.DomainEntryDiff{Key: "key388888888", Value: []byte("value3"), PrevStepBytes: step3[:]})
	d = append(d, kv.DomainEntryDiff{Key: "key388888888", Value: []byte("value3"), PrevStepBytes: step1[:]})

	serialized := SerializeDiffSet(d, nil)
	fmt.Println(len(serialized))
	deserialized := DeserializeDiffSet(serialized)

	require.Equal(t, d, deserialized)
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

	merged := MergeDiffSets(d1, d2)
	require.Len(t, merged, 4)

	require.Equal(t, d2[0], merged[0])
	require.Equal(t, d1[1], merged[1])
	require.Equal(t, d2[1], merged[2])
	require.Equal(t, d2[2], merged[3])
}
