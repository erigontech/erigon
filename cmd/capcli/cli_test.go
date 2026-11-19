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

package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// beaconAPIRequests are the request helpers of the older capcli commands, each taking the URL it requests.
func beaconAPIRequests(t *testing.T) map[string]func(uri string) error {
	return map[string]func(base string) error{
		"block": func(base string) error {
			_, err := retrieveAndSanitizeBlockFromRemoteEndpoint(t.Context(), &clparams.MainnetBeaconConfig, base, nil)
			return err
		},
		"state": func(base string) error {
			_, err := getBeaconState(t.Context(), &clparams.MainnetBeaconConfig, base, 0)
			return err
		},
		"head": func(base string) error {
			_, err := getHead(t.Context(), base)
			return err
		},
		"state root": func(base string) error {
			_, err := getStateRootAtSlot(t.Context(), base, 0)
			return err
		},
		"benchmark": func(base string) error {
			_, err := timeRequest(t.Context(), base, "application/json", "GET", "")
			return err
		},
	}
}

func TestBeaconAPIRequestsKeepTheEndpointSecretOutOfErrors(t *testing.T) {
	closed, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := closed.Addr().String()
	require.NoError(t, closed.Close())

	requests := beaconAPIRequests(t)
	for name, request := range requests {
		t.Run(name, func(t *testing.T) {
			err := request("http://user:pw-secret@" + host + "/path-secret")
			require.Error(t, err)
			for _, secret := range []string{"pw-secret", "path-secret", "user"} {
				require.NotContains(t, err.Error(), secret)
			}
			require.Contains(t, err.Error(), host)

			for _, malformed := range []string{"http://h/%zz/path-secret", "http://user:Ab3x/path-secret@h/"} {
				err = request(malformed)
				require.Error(t, err)
				require.NotContains(t, err.Error(), "path-secret")
				require.NotContains(t, err.Error(), "Ab3x", "a parse error quotes the malformed part")
			}
		})
	}
}

// A malformed redirect is quoted by net/http, and a server can repeat the request's path and query in it.
func TestBeaconAPIRequestsKeepTheEndpointSecretOutOfMalformedRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", r.URL.Path+"/%zz?"+r.URL.RawQuery)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	for name, request := range beaconAPIRequests(t) {
		t.Run(name, func(t *testing.T) {
			err := request(srv.URL + "/path-secret?apikey=query-secret")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "path-secret")
			require.NotContains(t, err.Error(), "query-secret")
		})
	}
}

// notADatadir is a regular file: a command that touches its datadir fails on it.
func notADatadir(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	return path
}

var malformedEndpoints = []string{"beacon.example:5052/?apikey=key-secret", "ftp://beacon.example/key-secret", "http:///key-secret", "http://h/%zz/key-secret", "http://user:key-secret/x@h/", "http://:5052/key-secret"}

// A malformed --endpoint stops the fill before it touches the datadir, and the error names the flag by position so a
// key in the value stays out of it.
func TestBlobArchiveStoreFillRefusesAMalformedEndpoint(t *testing.T) {
	for _, endpoint := range malformedEndpoints {
		fill := &BlobArchiveStoreFill{
			chainCfg:     chainCfg{Chain: "mainnet"},
			outputFolder: outputFolder{Datadir: notADatadir(t)},
			Endpoints:    []string{"https://beacon.example", endpoint},
		}
		err := fill.Run(t.Context())
		require.ErrorContains(t, err, "--endpoint 2", endpoint)
		require.NotContains(t, err.Error(), "key-secret")
	}
}

func TestChainEndpointRefusesAMalformedEndpoint(t *testing.T) {
	for _, endpoint := range malformedEndpoints {
		chain := &ChainEndpoint{chainCfg: chainCfg{Chain: "mainnet"}, outputFolder: outputFolder{Datadir: notADatadir(t)}, Endpoint: endpoint}
		err := chain.Run(t.Context())
		require.ErrorContains(t, err, "--endpoint", endpoint)
		require.NotContains(t, err.Error(), "key-secret")
	}
}

// TestHandleCheckersDetectOpenResources verifies that the cross-platform MDBX probe and
// Linux /proc/self checks catch open Caplin DB and snapshot handles before Close(), and
// clear once closed without cancelling the parent context.
func TestHandleCheckersDetectOpenResources(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dirs := datadir.New(t.TempDir())
	writeCapcliSnapshotSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, func(uint64) []byte { return []byte{1} })

	db, _, err := caplin1.OpenCaplinDatabase(ctx, dirs.CaplinIndexing, dirs.CaplinBlobs, false)
	require.NoError(t, err)
	closedDB := false
	defer func() {
		if !closedDB {
			db.Close()
		}
	}()

	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(networkname.Mainnet)
	require.NoError(t, err)
	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = networkname.Mainnet
	csn := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	require.NoError(t, csn.OpenFolder())
	closedSnap := false
	defer func() {
		if !closedSnap {
			csn.Close()
		}
	}()

	if runtime.GOOS == "linux" {
		require.NotEmpty(t, openPathsUnder(t, dirs.CaplinIndexing), "expected open handles for CaplinIndexing while db is open")
		require.NotEmpty(t, openPathsUnder(t, dirs.Snap), "expected open handles for Snap while csn is open")
	}
	probeWhileOpen, err := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dirs.CaplinIndexing).Exclusive(true).Open(ctx)
	if err == nil {
		probeWhileOpen.Close()
	}
	require.Error(t, err, "expected Exclusive(true) to reject already-open MDBX")

	csn.Close()
	closedSnap = true
	db.Close()
	closedDB = true

	// Assert BEFORE cancel() so we prove Close() released handles rather than ctx cancellation.
	assertMDBXClosed(t, dbcfg.CaplinDB, dirs.CaplinIndexing)
	assertDirHandlesClosed(t, dirs.Snap)
}

func TestCommandsCloseHandlesBeforeContextCancel(t *testing.T) {
	dummyWord := func(uint64) []byte { return []byte{1} }

	type commandCase struct {
		name          string
		wantErr       bool
		needSalt      bool
		needBeaconSeg bool
		needBlobSeg   bool
		needStateSeg  bool
		needEL        bool
		prepare       func(t *testing.T, ctx context.Context, dirs datadir.Dirs)
		afterRun      func(t *testing.T, err error)
		run           func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error
	}

	var checkpointHit atomic.Bool

	cases := []commandCase{
		{
			name:     "DumpSnapshots",
			needSalt: true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &DumpSnapshots{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "CheckSnapshots",
			wantErr:       true,
			needBeaconSeg: true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &CheckSnapshots{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "LoopSnapshots",
			needBeaconSeg: true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &LoopSnapshots{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "RetrieveHistoricalState",
			wantErr:       true,
			needBeaconSeg: true,
			needStateSeg:  true,
			needEL:        true,
			prepare: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) {
				checkpointHit.Store(false)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					checkpointHit.Store(true)
					http.NotFound(w, r)
				}))
				t.Cleanup(srv.Close)
				prevURLs := clparams.ConfigurableCheckpointsURLs
				clparams.ConfigurableCheckpointsURLs = []string{srv.URL}
				t.Cleanup(func() {
					clparams.ConfigurableCheckpointsURLs = prevURLs
				})
			},
			afterRun: func(t *testing.T, err error) {
				require.True(t, checkpointHit.Load(), "expected RetrieveHistoricalState to reach checkpoint sync after opening all DBs and snapshot folders (got err=%v)", err)
			},
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &RetrieveHistoricalState{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "BlobArchiveStoreCheck",
			needBeaconSeg: true,
			needBlobSeg:   true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &BlobArchiveStoreCheck{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "DumpBlobsSnapshots",
			needSalt:      true,
			needBeaconSeg: true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &DumpBlobsSnapshots{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "CheckBlobsSnapshots",
			needBeaconSeg: true,
			needBlobSeg:   true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &CheckBlobsSnapshots{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "CheckBlobsSnapshotsCount",
			needBeaconSeg: true,
			needBlobSeg:   true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &CheckBlobsSnapshotsCount{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:          "DumpBlobsSnapshotsToStore",
			needBeaconSeg: true,
			needBlobSeg:   true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &DumpBlobsSnapshotsToStore{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
				}
				return cmd.Run(ctx)
			},
		},
		{
			name:         "DumpStateSnapshots",
			needSalt:     true,
			needStateSeg: true,
			run: func(t *testing.T, ctx context.Context, dirs datadir.Dirs) error {
				cmd := &DumpStateSnapshots{
					chainCfg:     chainCfg{Chain: networkname.Mainnet},
					outputFolder: outputFolder{Datadir: dirs.DataDir},
					StepSize:     snaptype.CaplinMergeLimit,
				}
				return cmd.Run(ctx)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			dirs := datadir.New(t.TempDir())
			if tc.needSalt {
				_, err := snaptype.LoadSalt(dirs.Snap, true, log.New())
				require.NoError(t, err)
			}
			if tc.needBeaconSeg {
				writeCapcliSnapshotSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, dummyWord)
			}
			if tc.needBlobSeg {
				writeCapcliSnapshotSegment(t, dirs, snaptype.BlobSidecars, 0, snaptype.CaplinMergeLimit, dummyWord)
			}
			if tc.needStateSeg {
				writeCaplinStateSnapshotFixture(t, dirs, kv.BlockRoot, 0, snaptype.CaplinMergeLimit)
			}
			if tc.needEL {
				_, err := dbstate.GetStateIndicesSalt(dirs, true, log.New())
				require.NoError(t, err)
				writeELHeaderSnapshotFixture(t, dirs, 0, snaptype.CaplinMergeLimit)
				chainDB, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).Open(ctx)
				require.NoError(t, err)
				chainDB.Close()
			}
			if tc.prepare != nil {
				tc.prepare(t, ctx, dirs)
			}

			err := tc.run(t, ctx, dirs)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.afterRun != nil {
				tc.afterRun(t, err)
			}

			// Verify handles are closed BEFORE ctx is cancelled.
			assertMDBXClosed(t, dbcfg.CaplinDB, dirs.CaplinIndexing)
			if tc.needEL {
				assertMDBXClosed(t, dbcfg.ChainDB, dirs.Chaindata)
			}
			assertDirHandlesClosed(t, dirs.Snap)
		})
	}
}

func assertMDBXClosed(t *testing.T, label kv.Label, path string) {
	t.Helper()
	assertDirHandlesClosed(t, path)

	probe, err := mdbx.New(label, log.New()).Path(path).Exclusive(true).Open(t.Context())
	require.NoError(t, err, "expected MDBX at %s to be closed before ctx cancellation", path)
	probe.Close()
}

func assertDirHandlesClosed(t *testing.T, root string) {
	t.Helper()
	if runtime.GOOS == "linux" {
		require.Empty(t, openPathsUnder(t, root), "expected zero open fds/mmaps under %s before ctx cancellation", root)
	}
	if runtime.GOOS == "windows" {
		require.NoError(t, dir.RemoveAll(root), "expected %s to be removable on Windows before ctx cancellation", root)
	}
}

func openPathsUnder(t *testing.T, root string) []string {
	t.Helper()
	cleanRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		cleanRoot = filepath.Clean(root)
	}
	prefix := cleanRoot + string(filepath.Separator)
	matchesPath := func(p string) bool {
		p = strings.TrimSuffix(p, " (deleted)")
		if resolved, err := filepath.EvalSymlinks(p); err == nil {
			p = resolved
		}
		p = filepath.Clean(p)
		return p == cleanRoot || strings.HasPrefix(p, prefix)
	}

	var leaks []string
	const procFDDir = "/proc/self/fd"
	fdEntries, err := os.ReadDir(procFDDir)
	require.NoError(t, err)
	for _, entry := range fdEntries {
		target, err := os.Readlink(procFDDir + "/" + entry.Name())
		if err != nil {
			continue
		}
		if matchesPath(target) {
			leaks = append(leaks, "fd:"+entry.Name()+"->"+target)
		}
	}

	mapsData, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(mapsData), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		pathField := strings.Join(fields[5:], " ")
		if matchesPath(pathField) {
			leaks = append(leaks, "mmap:"+pathField)
		}
	}
	return leaks
}

func writeELHeaderSnapshotFixture(t *testing.T, dirs datadir.Dirs, from, to uint64) {
	t.Helper()
	name := snaptype2.Headers.Enum()
	headerVer := snaptype2.Headers.Versions().Current
	idxVer := snaptype2.Indexes.HeaderHash.Version.Current
	segPath := filepath.Join(dirs.Snap, snaptype.SegmentFileName(headerVer, from, to, name))
	idxPath := filepath.Join(dirs.Snap, snaptype.IdxFileName(idxVer, from, to, name.String()))
	writeRecSplitSegmentFixture(t, segPath, idxPath, dirs.Tmp, 0)
}

func writeCaplinStateSnapshotFixture(t *testing.T, dirs datadir.Dirs, table string, from, to uint64) {
	t.Helper()
	segName := strings.ReplaceAll(snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, to), "beaconblocks", table)
	segPath := filepath.Join(dirs.SnapCaplin, segName)
	idxPath := strings.TrimSuffix(segPath, ".seg") + ".idx"
	writeRecSplitSegmentFixture(t, segPath, idxPath, dirs.Tmp, from)
}

func writeRecSplitSegmentFixture(t *testing.T, segPath, idxPath, tmpDir string, baseDataID uint64) {
	t.Helper()
	logger := log.New()
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, tmpDir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord([]byte{1}))
	require.NoError(t, c.Compress())

	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     tmpDir,
		IndexFile:  idxPath,
		LeafSize:   8,
		BaseDataID: baseDataID,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	require.NoError(t, idx.AddKey([]byte{1}, 0))
	require.NoError(t, idx.Build(t.Context()))
}
