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

type fakeVisibleFile struct {
	path                 string
	startTxNum, endTxNum uint64
	version              version.Version
}

func (f fakeVisibleFile) Fullpath() string         { return f.path }
func (f fakeVisibleFile) StartRootNum() uint64     { return f.startTxNum }
func (f fakeVisibleFile) EndRootNum() uint64       { return f.endTxNum }
func (f fakeVisibleFile) Version() version.Version { return f.version }

// accountBranch builds a single-cell BranchData carrying one account-addr key: touchMap=afterMap=1,
// fieldBits=fieldAccountAddr(2), then uvarint(len)+addr. A 20-byte addr is plain; a shorter one is a
// shortened (offset) reference.
func accountBranch(addr []byte) []byte {
	b := []byte{0, 1, 0, 1, 0x02}
	var n [binary.MaxVarintLen64]byte
	c := binary.PutUvarint(n[:], uint64(len(addr)))
	b = append(b, n[:c]...)
	return append(b, addr...)
}

// writeCommitmentKV writes a tiny commitment .kv (state record + one account branch) with the
// commitment domain's compression. referencedContent => the branch carries a shortened key.
func writeCommitmentKV(t *testing.T, referencedContent bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v2.1-commitment.0-2.kv")
	comp, err := seg.NewCompressor(t.Context(), "test", path, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	w := seg.NewWriter(comp, statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression)

	_, err = w.Write(commitmentdb.KeyCommitmentState)
	require.NoError(t, err)
	_, err = w.Write([]byte("state-blob"))
	require.NoError(t, err)

	addr := make([]byte, length.Addr) // plain 20-byte account key
	if referencedContent {
		addr = []byte{0x81, 0x02} // shortened (varint offset) key
	}
	_, err = w.Write([]byte("\x01"))
	require.NoError(t, err)
	_, err = w.Write(accountBranch(addr))
	require.NoError(t, err)

	require.NoError(t, comp.Compress())
	comp.Close()
	return path
}

// TestCommitmentFileReferencing pins that the integrity referencing decision is taken from a FULL
// content scan of the file, independent of the file's version stamp — so it catches a file whose
// version says plain (v2.1 below) yet whose content carries shortened keys.
func TestCommitmentFileReferencing(t *testing.T) {
	t.Run("referenced content is referencing even when version says plain", func(t *testing.T) {
		f := fakeVisibleFile{path: writeCommitmentKV(t, true), endTxNum: 20, version: version.V2_1}
		require.True(t, commitmentFileReferencing(f))
	})
	t.Run("plain content is not referencing even when version says referenced", func(t *testing.T) {
		f := fakeVisibleFile{path: writeCommitmentKV(t, false), endTxNum: 20, version: version.V2_0}
		require.False(t, commitmentFileReferencing(f))
	})
}
