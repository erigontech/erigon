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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/state/valfile"
)

// commitment-like cfg (LargeValues:false, DupSort) with external value-files on.
func valFileTestDomainCfg() statecfg.DomainCfg {
	cfg := statecfg.Schema.CommitmentDomain
	cfg.ValueFileThreshold = 256
	return cfg
}

func TestValFiles_WriteFlushGetLatest_AndCollate(t *testing.T) {
	t.Parallel()
	logger := log.New()
	db, d := testDbAndDomainOfStep(t, valFileTestDomainCfg(), 16, logger)
	require.NotNil(t, d.valFiles, "external value-files must be enabled")
	ctx := t.Context()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	dt := d.beginForTests()
	defer dt.Close()
	w := dt.NewWriter()
	defer w.Close()

	// step 0 (aggStep=16): one large value (externalized) and one small (inline)
	bigKey := []byte("big-branch-key")
	bigVal := bytes.Repeat([]byte{0xcd}, 1024) // >= 256 -> goes to .cvl
	smallKey := []byte("small-branch-key")
	smallVal := []byte("tiny") // < 256 -> stays inline in MDBX

	require.NoError(t, w.PutWithPrev(bigKey, bigVal, 2, nil))
	require.NoError(t, w.PutWithPrev(smallKey, smallVal, 3, nil))
	require.NoError(t, w.Flush(ctx, tx))

	// round-trip through MDBX + value-file
	gotBig, _, ok, err := dt.GetLatest(bigKey, tx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, bigVal, gotBig)

	gotSmall, _, ok, err := dt.GetLatest(smallKey, tx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, smallVal, gotSmall)

	// collate step 0 -> frozen .kv must contain the REAL bytes (handles resolved)
	coll, err := d.collate(ctx, 0, 0, d.stepSize, tx)
	require.NoError(t, err)
	defer coll.Close()
	require.NoError(t, coll.valuesComp.Compress())

	dec, err := seg.NewDecompressor(coll.valuesPath)
	require.NoError(t, err)
	defer dec.Close()

	found := map[string][]byte{}
	g := dec.MakeGetter()
	for g.HasNext() {
		k, _ := g.Next(nil)
		v, _ := g.Next(nil)
		found[string(k)] = append([]byte{}, v...)
	}
	require.Equal(t, bigVal, found[string(bigKey)], "large value must be inlined into the .kv by collate")
	require.Equal(t, smallVal, found[string(smallKey)])
}

// A sealed .cvl becomes a FilesItem (with a valfile.Reader) surfaced into the
// per-domain visible map; retire removes it and hands it back for reader-safe
// deletion via closeFilesAndRemove.
func TestValFiles_SealedItemVisibleAndRetirable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m := newDomainValFiles(kv.CommitmentDomain, dir, "commitment", "v1.0", 256, 0, false, 16, 32, log.New())

	bigVal := bytes.Repeat([]byte{0x7a}, 1024)
	payload, err := m.encodeAppend(nil, 0, []byte("k"), bigVal)
	require.NoError(t, err)
	require.NoError(t, m.sync())
	require.NoError(t, m.ensureFilesItems()) // publish the per-step FilesItem
	h, _, err := valfile.DecodeHandle(payload[1:])
	require.NoError(t, err)

	vis := m.visibleValFiles()
	item := vis[0]
	require.NotNil(t, item, "step 0 must be visible")
	require.NotNil(t, item.valReader)
	got, err := item.valReader.Get(h, nil)
	require.NoError(t, err)
	require.Equal(t, bigVal, got)

	path := m.filePath(0)
	require.FileExists(t, path)

	retired := m.retire(0)
	require.Same(t, item, retired, "retire returns the same FilesItem")
	require.Nil(t, m.visibleValFiles()[0], "retired item no longer visible")
	require.FileExists(t, path, "retire must not delete the file itself")

	retired.closeFilesAndRemove() // the reclaimer's job, after readers drain
	require.NoFileExists(t, path)
}

// On restart openFolder deletes orphan .cvl (steps already in a frozen .kv) and
// reopens the live ones as readable FilesItems.
func TestValFiles_OpenFolderRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	big := bytes.Repeat([]byte{0x11}, 1024)

	m := newDomainValFiles(kv.CommitmentDomain, dir, "commitment", "v1.0", 256, 0, false, 16, 32, log.New())
	payloads := map[kv.Step][]byte{}
	for _, s := range []kv.Step{0, 1, 2} {
		p, err := m.encodeAppend(nil, s, []byte("k"), big)
		require.NoError(t, err)
		payloads[s] = p
	}
	require.NoError(t, m.sync())
	require.NoError(t, m.ensureFilesItems())
	m.Close() // shutdown: closes fds, files remain on disk

	// restart: firstStepNotInFiles=1 => step 0 is collated+pruned (orphan)
	m2 := newDomainValFiles(kv.CommitmentDomain, dir, "commitment", "v1.0", 256, 0, false, 16, 32, log.New())
	defer m2.Close()
	require.NoError(t, m2.openFolder(1, log.New()))

	require.NoFileExists(t, m2.filePath(0), "orphan step 0 must be deleted")
	require.FileExists(t, m2.filePath(1))

	vis := m2.visibleValFiles()
	require.Nil(t, vis[0])
	require.NotNil(t, vis[1], "live step 1 reopened")
	require.NotNil(t, vis[2], "live step 2 reopened")

	h, _, err := valfile.DecodeHandle(payloads[1][1:])
	require.NoError(t, err)
	got, err := vis[1].valReader.Get(h, nil)
	require.NoError(t, err)
	require.Equal(t, big, got)
}

func TestValFiles_DisabledIsByteIdenticalPath(t *testing.T) {
	t.Parallel()
	logger := log.New()
	cfg := statecfg.Schema.CommitmentDomain
	cfg.ValueFileThreshold = 0 // disabled, regardless of any COMMITMENT_VALFILE_THRESHOLD env override
	db, d := testDbAndDomainOfStep(t, cfg, 16, logger)
	require.Nil(t, d.valFiles, "value-files must be disabled when threshold==0")
	ctx := t.Context()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dt := d.beginForTests()
	defer dt.Close()
	w := dt.NewWriter()
	defer w.Close()

	k := []byte("k")
	v := bytes.Repeat([]byte{0x01}, 1024)
	require.NoError(t, w.PutWithPrev(k, v, 2, nil))
	require.NoError(t, w.Flush(ctx, tx))

	got, _, ok, err := dt.GetLatest(k, tx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, v, got)
}
