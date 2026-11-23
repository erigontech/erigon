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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

func testSegFile(t *testing.T, name string) *seg.Decompressor {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	c, err := seg.NewCompressor(t.Context(), "", path, dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.AddWord([]byte("word")))
	require.NoError(t, c.Compress())
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return d
}

func commitmentVisible(items ...*FilesItem) *aggregatorVisible {
	files := make(visibleFiles, len(items))
	for i, it := range items {
		files[i] = visibleFile{startTxNum: it.startTxNum, endTxNum: it.endTxNum, src: it}
	}
	v := &aggregatorVisible{}
	v.d[kv.CommitmentDomain] = &domainVisible{files: files, name: kv.CommitmentDomain}
	return v
}

func TestAggregatorLocksTheTwoCommitmentFilesBeforeTheNewest(t *testing.T) {
	items := make([]*FilesItem, 5)
	for i := range items {
		items[i] = &FilesItem{decompressor: testSegFile(t, fmt.Sprintf("c%d.kv", i)), startTxNum: uint64(i), endTxNum: uint64(i + 1)}
	}
	a := &Aggregator{}

	a.lockHotCommitmentFiles(commitmentVisible(items[:4]...))
	require.Equal(t, map[*FilesItem]struct{}{items[1]: {}, items[2]: {}}, a.lockedCommitment)

	a.lockHotCommitmentFiles(commitmentVisible(items...))
	require.Equal(t, map[*FilesItem]struct{}{items[2]: {}, items[3]: {}}, a.lockedCommitment, "a new newest file shifts the pair")

	a.lockHotCommitmentFiles(commitmentVisible(items[0], items[4]))
	require.Equal(t, map[*FilesItem]struct{}{items[0]: {}}, a.lockedCommitment, "files that left the visible set drop out")

	a.lockHotCommitmentFiles(commitmentVisible(items[4]))
	require.Empty(t, a.lockedCommitment)
}
