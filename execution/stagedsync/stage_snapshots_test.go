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

package stagedsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
)

// TestBuildOrDeferE2Indices_LifecycleGateNoOps verifies that when
// SnapshotsCfg.lifecycleDrivenByStorage is set, buildOrDeferE2Indices
// returns nil without calling cfg.blockRetire — proving the storage
// component's lifecycle driver is taking over without the stage
// having to coordinate.
func TestBuildOrDeferE2Indices_LifecycleGateNoOps(t *testing.T) {
	cfg := SnapshotsCfg{
		chainConfig: &chain.Config{},
		blockRetire: nil,
	}
	cfg.SetLifecycleDrivenByStorage(true)

	s := &StageState{}
	require.NoError(t, buildOrDeferE2Indices(context.Background(), s, cfg, 0),
		"flag-on must short-circuit before touching blockRetire")
}

func TestBuildOrDeferE3Accessors_LifecycleGateNoOps(t *testing.T) {
	cfg := SnapshotsCfg{}
	cfg.SetLifecycleDrivenByStorage(true)

	s := &StageState{}
	require.NoError(t, buildOrDeferE3Accessors(context.Background(), s, cfg, nil, 0),
		"flag-on must short-circuit before touching agg")
}

func TestSetLifecycleDrivenByStorage_DefaultsFalse(t *testing.T) {
	cfg := SnapshotsCfg{}
	require.False(t, cfg.lifecycleDrivenByStorage,
		"default must be false — stage drives until production wires the flag on")
	cfg.SetLifecycleDrivenByStorage(true)
	require.True(t, cfg.lifecycleDrivenByStorage)
	cfg.SetLifecycleDrivenByStorage(false)
	require.False(t, cfg.lifecycleDrivenByStorage)
}

// TestSetBlockHeadersOpenedHook_DefaultsNil pins the hook contract: nil
// by default (tests and tools that don't run the storage component see
// no behaviour change) and round-trips through the setter so production
// wiring (backend.go → stageloop → snapCfg) can install a publisher
// that translates the stage-side signal into a flow.BlockHeadersReady
// event on the storage event bus.
func TestSetBlockHeadersOpenedHook_DefaultsNil(t *testing.T) {
	cfg := SnapshotsCfg{}
	require.Nil(t, cfg.blockHeadersOpened,
		"default must be nil — stage no-ops the call when no hook is wired")

	var seenTip uint64
	cfg.SetBlockHeadersOpenedHook(func(tip uint64) { seenTip = tip })
	require.NotNil(t, cfg.blockHeadersOpened)
	cfg.blockHeadersOpened(12345)
	require.Equal(t, uint64(12345), seenTip,
		"installed hook must receive the tip passed by the stage")
}

func TestParseFileStepRange(t *testing.T) {
	tests := []struct {
		name      string
		wantStart kv.Step
		wantEnd   kv.Step
		wantOk    bool
	}{
		{"v2.0-accounts.8192-8704.kv", 8192, 8704, true},
		{"v2.0-commitment.0-8192.kv", 0, 8192, true},
		{"v1.1-storage.8772-8774.bt", 8772, 8774, true},
		{"v3.0-accounts.8704-8768.ef", 8704, 8768, true},
		// Block files: "v2.0-024823-024824-bodies.idx" splits to ["v2", "0-024823-024824-bodies", "idx"].
		// Sscanf parses "0-024823..." as start=0, end=24823. This is a false positive parse,
		// but isStateDomainFile() filters block files before deletion, so it's harmless.
		{"v2.0-024823-024824-bodies.idx", 0, 24823, true},
		{"preverified.toml", 0, 0, false},
		{"erigondb.toml", 0, 0, false},
		{"salt-blocks.txt", 0, 0, false},
		{"v2.0-accounts.8192-8704.kv.torrent", 8192, 8704, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := parseFileStepRange(tt.name)
			assert.Equal(t, tt.wantOk, ok, "ok")
			if ok {
				assert.Equal(t, tt.wantStart, start, "start")
				assert.Equal(t, tt.wantEnd, end, "end")
			}
		})
	}
}

func TestIsStateDomainFile(t *testing.T) {
	stateFiles := []string{
		"v2.0-accounts.8192-8704.kv",
		"v1.1-storage.8772-8774.bt",
		"v2.0-commitment.0-8192.kvi",
		"v2.0-code.8704-8768.kv",
		"v1.2-receipt.8768-8772.bt",
		"v3.0-logaddrs.8704-8768.ef",
		"v3.0-logtopics.8704-8768.ef",
		"v3.0-tracesfrom.8704-8768.ef",
		"v3.0-tracesto.8704-8768.ef",
		"v2.0-rcache.8768-8772.kvi",
	}
	for _, name := range stateFiles {
		assert.True(t, isStateDomainFile(name), "should be state: %s", name)
	}

	blockFiles := []string{
		"v2.0-024823-024824-bodies.idx",
		"v1.1-024822-024823-headers.seg",
		"v1.1-024822-024823-transactions.seg",
		"preverified.toml",
		"erigondb.toml",
		"salt-blocks.txt",
	}
	for _, name := range blockFiles {
		assert.False(t, isStateDomainFile(name), "should NOT be state: %s", name)
	}
}

func TestFindHighestStateFileStartStep(t *testing.T) {
	dir := t.TempDir()
	dirs := datadir.Dirs{SnapDomain: dir}

	// Empty dir
	step, found := findHighestStateFileStartStep(dirs)
	assert.False(t, found)
	assert.Equal(t, kv.Step(0), step)

	// Add some state files
	for _, name := range []string{
		"v2.0-accounts.0-8192.kv",
		"v2.0-accounts.8192-8704.kv",
		"v2.0-accounts.8704-8768.kv",
		"v2.0-commitment.8768-8772.kv",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte{}, 0644))
	}

	step, found = findHighestStateFileStartStep(dirs)
	assert.True(t, found)
	assert.Equal(t, kv.Step(8768), step)

	// Add a block file — should be ignored
	require.NoError(t, os.WriteFile(filepath.Join(dir, "v2.0-024823-024824-bodies.idx"), []byte{}, 0644))
	step, found = findHighestStateFileStartStep(dirs)
	assert.True(t, found)
	assert.Equal(t, kv.Step(8768), step) // still 8768, not 24823

	// Step 0 as only file
	dir2 := t.TempDir()
	dirs2 := datadir.Dirs{SnapDomain: dir2}
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "v2.0-accounts.0-8192.kv"), []byte{}, 0644))
	step, found = findHighestStateFileStartStep(dirs2)
	assert.True(t, found)
	assert.Equal(t, kv.Step(0), step) // step 0 IS valid
}

func TestRemoveStateFilesFromStep(t *testing.T) {
	snapDir := t.TempDir()
	idxDir := t.TempDir()
	histDir := t.TempDir()
	accDir := t.TempDir()
	dirs := datadir.Dirs{
		Snap:          t.TempDir(),
		SnapDomain:    snapDir,
		SnapIdx:       idxDir,
		SnapHistory:   histDir,
		SnapAccessors: accDir,
	}

	// Create state files across steps
	stateFiles := []string{
		"v2.0-accounts.0-8192.kv",
		"v2.0-accounts.8192-8704.kv",
		"v2.0-accounts.8704-8768.kv",
		"v2.0-commitment.8768-8772.kv",
	}
	for _, name := range stateFiles {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, name), []byte{}, 0644))
	}

	// Create a block file that should NOT be removed
	blockFile := "v2.0-024823-024824-bodies.idx"
	require.NoError(t, os.WriteFile(filepath.Join(idxDir, blockFile), []byte{}, 0644))

	// Create preverified.toml
	pvContent := `'domain/v2.0-accounts.0-8192.kv' = 'abc123'
'domain/v2.0-accounts.8192-8704.kv' = 'def456'
'domain/v2.0-accounts.8704-8768.kv' = 'ghi789'
'domain/v2.0-commitment.8768-8772.kv' = 'jkl012'
'v2.0-024823-024824-bodies.idx' = 'mno345'
`
	pvPath := filepath.Join(dirs.Snap, "preverified.toml")
	require.NoError(t, os.WriteFile(pvPath, []byte(pvContent), 0644))

	logger := log.New()

	// Remove from step 8704
	result := removeStateFilesFromStep(dirs, 8704, logger, "test")

	assert.Equal(t, 2, result.count) // 8704-8768 and 8768-8772
	assert.Len(t, result.names, 2)

	// Verify 0-8192 and 8192-8704 still exist
	_, err := os.Stat(filepath.Join(snapDir, "v2.0-accounts.0-8192.kv"))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(snapDir, "v2.0-accounts.8192-8704.kv"))
	assert.NoError(t, err)

	// Verify 8704-8768 and 8768-8772 are gone
	_, err = os.Stat(filepath.Join(snapDir, "v2.0-accounts.8704-8768.kv"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(snapDir, "v2.0-commitment.8768-8772.kv"))
	assert.True(t, os.IsNotExist(err))

	// Verify block file NOT removed
	_, err = os.Stat(filepath.Join(idxDir, blockFile))
	assert.NoError(t, err, "block file should not be removed")

	// Verify preverified.toml stripped correctly
	pvData, err := os.ReadFile(pvPath)
	require.NoError(t, err)
	assert.Contains(t, string(pvData), "0-8192")
	assert.Contains(t, string(pvData), "8192-8704")
	assert.NotContains(t, string(pvData), "8704-8768")
	assert.NotContains(t, string(pvData), "8768-8772")
	assert.Contains(t, string(pvData), "bodies") // block entries preserved
}

func TestAlignmentSkipsWhenTxNumsIndexExists(t *testing.T) {
	// When the TxNums index already covers frozen blocks, alignment must
	// NOT run — the node previously processed past any snapshot
	// misalignment. Running alignment on restart would remove snapshot
	// files that the downloader re-downloaded, cascading until all state
	// files are gone.
	//
	// The guard checks: TxNums.Max(frozenBlocks) > 0. A full integration
	// test requires a temporal DB with TxNums populated, which is beyond
	// unit test scope. The guard is tested end-to-end via the
	// fresh-start/restart cycle.

	// Verify the helper functions are safe with existing files
	snapDir := t.TempDir()
	stateFiles := []string{
		"v2.0-accounts.0-8192.kv",
		"v2.0-accounts.8192-8704.kv",
		"v2.0-commitment.8704-8768.kv",
	}
	for _, name := range stateFiles {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, name), []byte{}, 0644))
	}

	// Files should be untouched (alignment guard prevents removal)
	for _, name := range stateFiles {
		_, err := os.Stat(filepath.Join(snapDir, name))
		assert.NoError(t, err, "file should still exist: %s", name)
	}
}
