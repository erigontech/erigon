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

package btindex

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func TestBtIndex_RSeek(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	dataPath := generateKV(t, tmp, 20, 10, 8, log.New(), seg.CompressNone)
	indexPath := strings.TrimSuffix(filepath.Base(dataPath), ".kv") + ".bt"
	indexPath = filepath.Join(tmp, indexPath)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, seg.CompressNone, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)
	getter := seg.NewReader(kv.MakeGetter(), seg.CompressNone)

	tests := []struct {
		name string
		key  []byte
		want []byte
	}{
		{name: "before first", key: nil, want: keys[0]},
		{name: "first key", key: keys[0], want: keys[1]},
		{name: "hit", key: keys[2], want: keys[3]},
		{name: "miss", key: append(bytes.Clone(keys[3]), 0), want: keys[4]},
		{name: "last", key: keys[len(keys)-1], want: nil},
		{name: "past end", key: append(bytes.Clone(keys[len(keys)-1]), 0xff), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur, err := bt.RSeek(getter, tt.key)
			require.NoError(t, err)
			if tt.want == nil {
				require.Nil(t, cur)
				return
			}
			require.NotNil(t, cur)
			defer cur.Close()
			require.Equal(t, tt.want, cur.Key())
		})
	}
}

func TestBtIndex_LSeek(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	dataPath := generateKV(t, tmp, 20, 10, 8, log.New(), seg.CompressNone)
	indexPath := strings.TrimSuffix(filepath.Base(dataPath), ".kv") + ".bt"
	indexPath = filepath.Join(tmp, indexPath)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, seg.CompressNone, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)
	getter := seg.NewReader(kv.MakeGetter(), seg.CompressNone)

	tests := []struct {
		name string
		key  []byte
		want []byte
	}{
		{name: "before first", key: nil, want: nil},
		{name: "first key", key: keys[0], want: nil},
		{name: "hit", key: keys[3], want: keys[2]},
		{name: "miss", key: append(bytes.Clone(keys[3]), 0), want: keys[3]},
		{name: "last key", key: keys[len(keys)-1], want: keys[len(keys)-2]},
		{name: "past end", key: append(bytes.Clone(keys[len(keys)-1]), 0xff), want: keys[len(keys)-1]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur, err := bt.LSeek(getter, tt.key)
			require.NoError(t, err)
			if tt.want == nil {
				require.Nil(t, cur)
				return
			}
			require.NotNil(t, cur)
			defer cur.Close()
			require.Equal(t, tt.want, cur.Key())
		})
	}
}

func TestBtIndex_LSeekEmpty(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	dataPath := generateKV(t, tmp, 20, 10, 0, log.New(), seg.CompressNone)
	indexPath := strings.TrimSuffix(filepath.Base(dataPath), ".kv") + ".bt"
	indexPath = filepath.Join(tmp, indexPath)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, seg.CompressNone, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewReader(kv.MakeGetter(), seg.CompressNone)
	cur, err := bt.RSeek(getter, []byte("key"))
	require.NoError(t, err)
	require.Nil(t, cur)

	cur, err = bt.LSeek(getter, []byte("key"))
	require.NoError(t, err)
	require.Nil(t, cur)
}
