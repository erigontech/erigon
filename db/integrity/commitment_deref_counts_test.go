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

package integrity

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// accountStorageBranch builds a single-cell BranchData carrying an account-addr key and a
// storage-addr key: touchMap=afterMap=1, fieldBits=fieldAccountAddr|fieldStorageAddr(6), then
// uvarint(len)+key for each.
func accountStorageBranch(addr, storageKey []byte) []byte {
	b := []byte{0, 1, 0, 1, 0x06}
	var n [binary.MaxVarintLen64]byte
	for _, key := range [][]byte{addr, storageKey} {
		c := binary.PutUvarint(n[:], uint64(len(key)))
		b = append(b, n[:c]...)
		b = append(b, key...)
	}
	return b
}

// writeCommitmentRecords writes a commitment .kv holding records verbatim, so an odd count leaves a
// key with no value — the truncation the scan has to notice.
func writeCommitmentRecords(t *testing.T, records ...[]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v2.1-commitment.0-2.kv")
	comp, err := seg.NewCompressor(t.Context(), "test", path, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	w := seg.NewWriter(comp, statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression)
	for _, r := range records {
		_, err = w.Write(r)
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
	return path
}

// TestCheckCommitmentKvDerefCounts pins that a file the referencing scan clears still reports the
// keys it walked. An all-zero tally is indistinguishable from having scanned nothing, so a run over
// a fully plain datadir would otherwise produce a summary that proves neither outcome.
func TestCheckCommitmentKvDerefCounts(t *testing.T) {
	t.Run("plain file reports what it walked", func(t *testing.T) {
		branch := accountStorageBranch(make([]byte, length.Addr), make([]byte, length.Addr+length.Hash))
		f := fakeVisibleFile{path: writeCommitmentRecords(t,
			commitmentdb.KeyCommitmentState, []byte("state-blob"),
			[]byte("\x01"), branch,
		), endTxNum: 20, version: version.V2_0}

		counts, err := checkCommitmentKvDeref(t.Context(), f, 10, true /* failFast */, log.New())
		require.NoError(t, err)
		require.Equal(t, derefCounts{branchKeys: 1, plainAccounts: 1, plainStorages: 1}, counts)
	})

	t.Run("referencing file reports no tally", func(t *testing.T) {
		f := fakeVisibleFile{path: writeCommitmentKV(t, true), endTxNum: 20, version: version.V2_1}
		scan := scanCommitmentFile(f)
		require.True(t, scan.referenced)
		require.Equal(t, derefCounts{}, scan.counts)
	})

	t.Run("a key with no value is referencing, not a cleared plain scan", func(t *testing.T) {
		f := fakeVisibleFile{path: writeCommitmentRecords(t,
			commitmentdb.KeyCommitmentState, []byte("state-blob"),
			[]byte("\x01"),
		), endTxNum: 20, version: version.V2_0}

		scan := scanCommitmentFile(f)
		require.True(t, scan.referenced, "the deref pass reports the dangling key as ErrIntegrity; the scan must not skip it")
		require.Equal(t, derefCounts{}, scan.counts)
	})
}
