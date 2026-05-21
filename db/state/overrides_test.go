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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, b, 0o644))
}

// TestDomainRoTx_substituteFile builds a live step-0 accounts file holding
// key→"live", then builds a second file (step 1, holding key→"staged")
// whose bytes are renamed into a staging directory as a step-0 file. The
// step-0 view substituted with the staged file must read "staged"; the
// untouched live view must still read "live".
func TestDomainRoTx_substituteFile(t *testing.T) {
	logger := log.New()
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)
	ctx := t.Context()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	key := []byte("account-key-0001")
	liveVal := []byte("live-value")
	stagedVal := []byte("staged-value")

	w := d.beginForTests()
	writer := w.NewWriter()
	require.NoError(t, writer.PutWithPrev(key, liveVal, 2, nil))
	require.NoError(t, writer.PutWithPrev(key, stagedVal, d.stepSize+2, liveVal))
	require.NoError(t, writer.Flush(ctx, tx))
	writer.Close()
	w.Close()

	ps := background.NewProgressSet()
	// Step 0 → the live file (key→"live"), integrated into dirtyFiles.
	require.NoError(t, d.collateBuildIntegrate(ctx, 0, tx, ps))
	// Step 1 → built but not integrated; its bytes (key→"staged") become
	// the staged file. A .kv's logical range is purely its filename and the
	// .bt / .kvei accessors are content-derived, so renaming 1-2 → 0-1
	// yields a well-formed step-0 file.
	c, err := d.collate(ctx, 1, d.stepSize, 2*d.stepSize, tx)
	require.NoError(t, err)
	sf, err := d.buildFiles(ctx, 1, c, ps)
	require.NoError(t, err)
	c.Close()

	staging := t.TempDir()
	copyFile(t, sf.valuesDecomp.FilePath(), filepath.Join(staging, filepath.Base(d.kvNewFilePath(0, 1))))
	copyFile(t, sf.valuesBt.FilePath(), filepath.Join(staging, filepath.Base(d.kvBtAccessorNewFilePath(0, 1))))
	copyFile(t, sf.existenceFilter.FilePath, filepath.Join(staging, filepath.Base(d.kvExistenceIdxNewFilePath(0, 1))))
	sf.CleanupOnError()

	// Baseline: the live view reads "live".
	dtLive := d.beginForTests()
	v, found, _, _, err := dtLive.getLatestFromFiles(key, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, liveVal, v)
	dtLive.Close()

	// Overridden view reads the staged bytes.
	fi, err := d.openDomainFileItemAt(staging, 0, 1)
	require.NoError(t, err)
	dtOverride := d.beginForTests()
	require.NoError(t, dtOverride.substituteFile(fi))
	require.True(t, strings.HasPrefix(dtOverride.files[0].src.decompressor.FilePath(), staging),
		"substituted file must resolve to the staging directory")
	v, found, _, _, err = dtOverride.getLatestFromFiles(key, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, stagedVal, v, "overridden view must read the staged bytes")
	dtOverride.Close()
	fi.closeFiles()

	// The live file is unaffected by the override.
	dtLive2 := d.beginForTests()
	v, found, _, _, err = dtLive2.getLatestFromFiles(key, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, liveVal, v, "live file must be unchanged after the override")
	dtLive2.Close()
}

func TestDomainRoTx_substituteFile_NoCoveringRange(t *testing.T) {
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)

	dt := d.beginForTests()
	defer dt.Close()

	// A FilesItem for a range with no live counterpart is rejected.
	fi := newFilesItem(5*d.stepSize, 6*d.stepSize, d.stepSize, d.stepsInFrozenFile)
	err := dt.substituteFile(fi)
	require.ErrorContains(t, err, "no live")
}

func TestOpenDomainFileItemAt_MissingFile(t *testing.T) {
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)

	_, err := d.openDomainFileItemAt(t.TempDir(), 0, 1)
	require.ErrorContains(t, err, "values file")
}

func TestBeginFilesRoWithOverrides_BadPaths(t *testing.T) {
	_, agg := testDbAndAggregatorv3(t, 16)

	_, err := agg.BeginFilesRoWithOverrides([]string{"garbage.txt"})
	require.ErrorContains(t, err, "not a parseable")

	_, err = agg.BeginFilesRoWithOverrides([]string{"v1.0-accounts.0-1.v"})
	require.ErrorContains(t, err, ".kv")

	// An empty override list yields an ordinary read-only view.
	ac, err := agg.BeginFilesRoWithOverrides(nil)
	require.NoError(t, err)
	ac.Close()
}
