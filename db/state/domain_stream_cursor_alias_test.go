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

package state

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestDomainLatestIterFileKeepsKeyAfterCursorReuse(t *testing.T) {
	logger := log.New()
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.StorageDomain, 16, logger)
	ctx := t.Context()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	rw := d.beginForTests()
	w := rw.NewWriter(db)
	const n = 250
	for i := uint64(1); i <= n; i++ {
		var k, v [8]byte
		binary.BigEndian.PutUint64(k[:], i)
		binary.BigEndian.PutUint64(v[:], i*7+1)
		require.NoError(t, w.PutWithPrev(k[:], v[:], i, nil))
		if i%10 == 0 {
			require.NoError(t, w.Flush(ctx, tx))
		}
	}
	require.NoError(t, w.Flush(ctx, tx))
	w.Close()
	rw.Close()
	collateAndMerge(t, tx, d, n)

	viewA := d.beginForTests()
	defer viewA.Close()
	viewB := d.beginForTests()
	defer viewB.Close()
	t.Logf("files in view: %d", len(viewA.files))

	drain := func(it *DomainLatestIterFile) (ks, vs [][]byte) {
		for it.HasNext() {
			var k, v []byte
			k, v, err = it.Next()
			require.NoError(t, err)
			ks, vs = append(ks, k), append(vs, v)
		}
		return
	}
	base, err := viewA.DebugRangeLatestFromFiles(nil, nil, -1)
	require.NoError(t, err)
	wantK, wantV := drain(base)
	base.Close()
	require.NotEmpty(t, wantK)
	t.Logf("expected keys: %d", len(wantK))

	a, err := viewA.DebugRangeLatestFromFiles(nil, nil, -1)
	require.NoError(t, err)
	defer a.Close()
	var gotK, gotV [][]byte
	for a.HasNext() && !(a.h.Len() == 0 && a.nextKey != nil) {
		var k, v []byte
		k, v, err = a.Next()
		require.NoError(t, err)
		gotK, gotV = append(gotK, k), append(gotV, v)
	}
	require.True(t, a.HasNext(), "A never reached the state heap-empty with pending key")
	t.Logf("A paused with %d returned, pending nextKey=%x", len(gotK), a.nextKey)

	var b *DomainLatestIterFile
	for j := 2; j < 40; j++ {
		seek := append(bytes.Clone(wantK[len(wantK)-j]), 0x00)
		b, err = viewB.DebugRangeLatestFromFiles(seek, nil, -1)
		require.NoError(t, err)
		b.Close()
	}
	b, err = viewB.DebugRangeLatestFromFiles(append(bytes.Clone(wantK[len(wantK)-5]), 0), nil, -1)
	require.NoError(t, err)
	defer b.Close()
	t.Logf("B opened; A.nextKey now=%x", a.nextKey)

	k, v := drain(a)
	gotK, gotV = append(gotK, k...), append(gotV, v...)
	require.Equal(t, len(wantK), len(gotK))
	for i := range wantK {
		require.Equal(t, wantK[i], gotK[i], "key %d", i)
		require.Equal(t, wantV[i], gotV[i], "val %d", i)
	}
}
