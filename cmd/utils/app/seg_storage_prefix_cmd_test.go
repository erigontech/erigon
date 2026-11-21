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

package app

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func spAddr(prefix ...byte) []byte {
	a := make([]byte, 20)
	copy(a, prefix)
	return a
}

func spStorageKey(address []byte, slot byte) []byte {
	k := make([]byte, 52)
	copy(k, address)
	k[51] = slot
	return k
}

func writeDomainKV(t *testing.T, path string, compression seg.FileCompression, keys [][]byte) {
	t.Helper()
	cfg := statecfg.DomainCompressCfg
	cfg.Workers = 1
	comp, err := seg.NewCompressor(t.Context(), t.Name(), path, t.TempDir(), cfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer comp.Close()
	comp.DisableFsync()
	w := seg.NewWriter(comp, compression)
	for _, k := range keys {
		_, err = w.Write(k)
		require.NoError(t, err)
		_, err = w.Write([]byte{1, 2, 3})
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
}

func TestStoragePrefixEstimate(t *testing.T) {
	dir := t.TempDir()

	a1 := spAddr(0x00, 0x00)
	a2 := spAddr(0x00, 0x01)
	a3 := spAddr(0x80)
	a4 := spAddr(0x80, 0x00, 0x00, 0x07)
	writeDomainKV(t, filepath.Join(dir, "v1.1-accounts.0-1.kv"), statecfg.Schema.AccountsDomain.Compression,
		[][]byte{a1, a2, a3, a4})

	noAccount1, noAccount2 := spAddr(0x40), spAddr(0xff)
	writeDomainKV(t, filepath.Join(dir, "v1.1-storage.0-1.kv"), statecfg.Schema.StorageDomain.Compression,
		[][]byte{
			spStorageKey(a1, 1), spStorageKey(a1, 2), spStorageKey(a1, 3),
			spStorageKey(noAccount1, 1), spStorageKey(noAccount1, 2),
			spStorageKey(a4, 1),
			spStorageKey(noAccount2, 1),
		})

	ranges, err := estimateStoragePrefixFiles(t.Context(), dir, "", 0, log.New())
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	r := ranges[0]
	require.Equal(t, uint64(0), r.From)
	require.Equal(t, uint64(1), r.To)

	s := r.Stats
	require.Equal(t, uint64(4), s.Accounts)
	require.Equal(t, uint64(2), s.AccountsWithStorage)
	require.Equal(t, uint64(7), s.StorageKeys)
	require.Equal(t, uint64(3), s.UnmatchedStorageKeys)
	require.Equal(t, uint64(2), s.UnmatchedAddresses)
	require.Equal(t, 4, s.MaxPrefixLen)
	// a1 and a2 need 2 bytes, a3 and a4 need 4 bytes
	require.Equal(t, uint64(2), s.AccountsByPrefixLen[2])
	require.Equal(t, uint64(2), s.AccountsByPrefixLen[4])
	require.Equal(t, uint64(3), s.SlotsByPrefixLen[2])
	require.Equal(t, uint64(1), s.SlotsByPrefixLen[4])
	// 3 slots of a1 save 18 bytes each, 1 slot of a4 saves 16 bytes
	require.Equal(t, uint64(70), s.SavedBytes)
	require.Equal(t, int64(70-4), s.NetSavedBytes())
	// one prefix length for the whole range: 4 matched slots save 20-4 bytes each
	require.Equal(t, uint64(64), s.UniformSavedBytes)

	recompressed, err := estimateStoragePrefixFiles(t.Context(), dir, t.TempDir(), 1, log.New())
	require.NoError(t, err)
	require.Len(t, recompressed, 1)
	require.Equal(t, s, recompressed[0].Stats)
	require.Positive(t, recompressed[0].ShortCompressedSize)
	require.Less(t, recompressed[0].ShortCompressedSize, recompressed[0].KeysCompressedSize)
}
