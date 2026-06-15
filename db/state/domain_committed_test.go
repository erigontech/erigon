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
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

const (
	fieldAccountAddr = byte(2)
	fieldStorageAddr = byte(4)
	fieldHash        = byte(8)
)

// branchWith builds a single-cell BranchData in the commitment wire format: 2-byte BE touchMap,
// 2-byte BE afterMap, then per set bit a fieldBits byte followed by uvarint(len)+bytes for each
// present field (account addr, then storage addr, then hash). Bit 0 is the only cell here.
func branchWith(fields byte, accountAddr, storageAddr, hash []byte) commitment.BranchData {
	var b []byte
	var m [2]byte
	binary.BigEndian.PutUint16(m[:], 1) // touchMap: bit 0
	b = append(b, m[:]...)
	binary.BigEndian.PutUint16(m[:], 1) // afterMap: bit 0
	b = append(b, m[:]...)
	b = append(b, fields)
	putKey := func(k []byte) {
		var n [binary.MaxVarintLen64]byte
		c := binary.PutUvarint(n[:], uint64(len(k)))
		b = append(b, n[:c]...)
		b = append(b, k...)
	}
	if fields&fieldAccountAddr != 0 {
		putKey(accountAddr)
	}
	if fields&fieldStorageAddr != 0 {
		putKey(storageAddr)
	}
	if fields&fieldHash != 0 {
		putKey(hash)
	}
	return b
}

func TestBranchHasShortenedKey(t *testing.T) {
	t.Parallel()
	hash := make([]byte, length.Hash)
	plainAddr := make([]byte, length.Addr)                // 20-byte account key
	plainStorage := make([]byte, length.Addr+length.Hash) // 52-byte storage key
	shortRef := []byte{0x81, 0x02}                        // varint file offset, never 20/52 bytes

	cases := []struct {
		name   string
		branch commitment.BranchData
		want   bool
	}{
		{"plain account key (20 bytes)", branchWith(fieldAccountAddr|fieldHash, plainAddr, nil, hash), false},
		{"plain storage key (52 bytes)", branchWith(fieldStorageAddr|fieldHash, nil, plainStorage, hash), false},
		{"short account key", branchWith(fieldAccountAddr|fieldHash, shortRef, nil, hash), true},
		{"short storage key", branchWith(fieldStorageAddr|fieldHash, nil, shortRef, hash), true},
		{"hash-only cell carries no key", branchWith(fieldHash, nil, nil, hash), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, branchHasShortenedKey(tc.branch))
		})
	}
}

// TestCommitmentFileReferenced writes tiny commitment .kv files and asserts the content sampler's
// verdict: a file whose branch values all carry plain keys is plain; one carrying a short key is
// referenced. The commitment-state record is skipped by the sampler and must not flip the verdict.
func TestCommitmentFileReferenced(t *testing.T) {
	t.Parallel()
	hash := make([]byte, length.Hash)
	plainAddr := make([]byte, length.Addr)
	shortRef := []byte{0x81, 0x02}

	write := func(t *testing.T, pairs [][2][]byte) *seg.Reader {
		t.Helper()
		path := filepath.Join(t.TempDir(), "v2.1-commitment.0-2.kv")
		comp, err := seg.NewCompressor(t.Context(), "test", path, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		for _, kv := range pairs {
			require.NoError(t, comp.AddWord(kv[0]))
			require.NoError(t, comp.AddWord(kv[1]))
		}
		require.NoError(t, comp.Compress())
		comp.Close()

		d, err := seg.NewDecompressor(path)
		require.NoError(t, err)
		t.Cleanup(d.Close)
		return seg.NewReader(d.MakeGetter(), seg.CompressNone)
	}

	plainBranch := []byte(branchWith(fieldAccountAddr|fieldHash, plainAddr, nil, hash))
	refBranch := []byte(branchWith(fieldAccountAddr|fieldHash, shortRef, nil, hash))
	stateVal := []byte("commitment-state-blob")

	t.Run("all-plain file is not referenced", func(t *testing.T) {
		r := write(t, [][2][]byte{
			{commitmentdb.KeyCommitmentState, stateVal},
			{[]byte("\x01"), plainBranch},
			{[]byte("\x02"), plainBranch},
		})
		require.False(t, commitmentFileReferenced(r, 3))
	})
	t.Run("file with a short key is referenced", func(t *testing.T) {
		r := write(t, [][2][]byte{
			{commitmentdb.KeyCommitmentState, stateVal},
			{[]byte("\x01"), plainBranch},
			{[]byte("\x02"), refBranch},
		})
		require.True(t, commitmentFileReferenced(r, 3))
	})
	t.Run("empty file is not referenced", func(t *testing.T) {
		r := write(t, nil)
		require.False(t, commitmentFileReferenced(r, 0))
	})
	t.Run("nil reader is not referenced", func(t *testing.T) {
		require.False(t, commitmentFileReferenced(nil, 5))
	})
}

// TestCommitmentFileReferencedStrideCompressKeys exercises the production read path the small fixture
// above does not: the commitment domain's CompressKeys compression and stride>1, where non-sampled
// pairs go through the Skip/Skip branch and must keep the key/value toggle aligned. It also pins the
// accepted bounded-sample limit: a short key at a non-sampled pair is missed.
func TestCommitmentFileReferencedStrideCompressKeys(t *testing.T) {
	t.Parallel()
	hash := make([]byte, length.Hash)
	plainAddr := make([]byte, length.Addr)
	shortRef := []byte{0x81, 0x02}
	plainBranch := []byte(branchWith(fieldAccountAddr|fieldHash, plainAddr, nil, hash))
	refBranch := []byte(branchWith(fieldAccountAddr|fieldHash, shortRef, nil, hash))

	// refAt < 0 => all branches plain; else the branch at that pair index carries a short key.
	writeKVKeys := func(t *testing.T, nPairs, refAt int) *seg.Reader {
		t.Helper()
		path := filepath.Join(t.TempDir(), "v2.1-commitment.0-2.kv")
		comp, err := seg.NewCompressor(t.Context(), "test", path, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		w := seg.NewWriter(comp, seg.CompressKeys)
		var key [4]byte
		for i := range nPairs {
			binary.BigEndian.PutUint32(key[:], uint32(i))
			_, err := w.Write(key[:])
			require.NoError(t, err)
			val := plainBranch
			if i == refAt {
				val = refBranch
			}
			_, err = w.Write(val)
			require.NoError(t, err)
		}
		require.NoError(t, comp.Compress())
		comp.Close()
		d, err := seg.NewDecompressor(path)
		require.NoError(t, err)
		t.Cleanup(d.Close)
		return seg.NewReader(d.MakeGetter(), seg.CompressKeys)
	}

	const n = 1000 // stride = n/200 = 5: sampled pairs 0,5,10,...; others take the Skip/Skip path

	t.Run("all-plain large file is not referenced", func(t *testing.T) {
		require.False(t, commitmentFileReferenced(writeKVKeys(t, n, -1), n))
	})
	t.Run("short key at a sampled pair is detected", func(t *testing.T) {
		require.True(t, commitmentFileReferenced(writeKVKeys(t, n, 500), n)) // 500 % 5 == 0
	})
	t.Run("short key only at a non-sampled pair is missed (bounded-sample limit)", func(t *testing.T) {
		require.False(t, commitmentFileReferenced(writeKVKeys(t, n, 501), n)) // 501 % 5 != 0
	})
}
