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

package engineapi_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

const (
	snapshotCrashChild      = "ERIGON_SNAPSHOT_CRASH_CHILD"
	snapshotCrashReorgDepth = 32
)

type snapshotCrashRequest struct {
	Genesis        *types.Genesis
	CoinbaseKey    []byte
	DataDir        string
	ControlAddress string
	Deadline       time.Time
	Canonical      []*engineapitester.MockClPayload
	Replacement    []*engineapitester.MockClPayload
	Boundary       state.MaintenancePoint
}

func configureSnapshotCrashRecovery(config *ethconfig.Config) {
	configureCrashRecovery(config)
	config.MaxReorgDepth = snapshotCrashReorgDepth
	config.Snapshot.ProduceE3 = true
}

// File publication and MDBX pruning must preserve the same logical state and
// history across a crash. Archive mode retains history in files, but still
// removes their MDBX copies; raw table counts prove that this removal took place.
func TestEngineApiSnapshotCrashRecovery(t *testing.T) {
	if os.Getenv(snapshotCrashChild) == "1" {
		runSnapshotCrashChild(t)
		return
	}
	if testing.Short() {
		t.Skip("subprocess snapshot recovery integration test")
	}
	ctx := t.Context()
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	baseArgs := engineapitester.EngineApiTesterInitArgs{
		Genesis: genesis, CoinbaseKey: key, NoEmptyBlock1: true,
		EthConfigTweaker: configureCrashRecovery,
	}
	const prefixPokes, suffixPokes = 48, 24
	buildReference := func(side bool) (prefix, tip crashRecoveryChain, addr common.Address, counts map[string]uint64) {
		args := baseArgs
		args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), newSmallStepDataDir(t)
		eat, err := engineapitester.InitialiseEngineApiTester(ctx, args)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, eat.Close()) })
		empty, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		payloads, addr, churn, sums := buildChurnChain(ctx, t, eat, prefixPokes, func(k int) int64 { return int64(k) })
		payloads = append([]*engineapitester.MockClPayload{empty}, payloads...)
		prefix = crashRecoveryChain{payloads: payloads, state: readCrashRecoveryState(t, eat.ChainDB), sum: sums[len(sums)-1]}
		suffix, sums := buildCrashRecoverySuffix(ctx, t, eat, churn, prefixPokes, suffixPokes, side)
		tip = crashRecoveryChain{payloads: append(payloads, suffix...), state: readCrashRecoveryState(t, eat.ChainDB), sum: sums[len(sums)-1]}
		counts = snapshotCrashTableCounts(t, eat.ChainDB)
		if side {
			tip.continuation, tip.continuationSums = churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(2_000 + k) })
		}
		require.NoError(t, eat.Close())
		return prefix, tip, addr, counts
	}
	prefix, canonical, addr, canonicalCounts := buildReference(false)
	sidePrefix, side, sideAddr, sideCounts := buildReference(true)
	require.Equal(t, addr, sideAddr)
	assertCrashRecoveryState(t, prefix.state, sidePrefix.state)
	assertCrashRecoveryReference(t, canonical)
	assertCrashRecoveryReference(t, side)
	for _, chain := range []crashRecoveryChain{canonical, side} {
		require.Less(t, chain.state.CommitmentBlock-prefix.state.CommitmentBlock, uint64(snapshotCrashReorgDepth),
			"replayed forks must stay inside the newPayload validation and changeset retention limits")
	}
	require.Equal(t, canonicalCounts, sideCounts, "the filed step must belong to the shared prefix")
	for table, count := range canonicalCounts {
		require.Positive(t, count, "%s must contain prunable records", table)
	}
	for _, domain := range []kv.Domain{kv.StorageDomain, kv.CodeDomain} {
		require.NotEqual(t, canonical.state.Domains[domain], side.state.Domains[domain], "forks must change %s differently", domain)
	}

	for _, window := range []struct {
		name           string
		point          state.MaintenancePoint
		reorgCommitted bool
		pruneCommitted bool
	}{
		{
			name:           "files_built",
			point:          state.MaintenanceFilesBuilt,
			reorgCommitted: false,
			pruneCommitted: false,
		},
		{
			name:           "files_published",
			point:          state.MaintenanceFilesPublished,
			reorgCommitted: false,
			pruneCommitted: false,
		},
		{
			name:           "prune_ready",
			point:          state.MaintenancePruneReady,
			reorgCommitted: true,
			pruneCommitted: false,
		},
		{
			name:           "prune_committed",
			point:          state.MaintenancePruneCommitted,
			reorgCommitted: true,
			pruneCommitted: true,
		},
	} {
		t.Run(window.name, func(t *testing.T) {
			request := snapshotCrashRequest{
				Genesis: genesis, CoinbaseKey: crypto.FromECDSA(key), DataDir: newSmallStepDataDir(t),
				Canonical: canonical.payloads, Replacement: side.payloads,
				Boundary: window.point,
			}
			killAtCrashBoundary(t, "TestEngineApiSnapshotCrashRecovery", snapshotCrashChild, int(window.point), func(address string, deadline time.Time) any {
				request.ControlAddress, request.Deadline = address, deadline
				return request
			})
			want := canonical
			if window.reorgCommitted {
				want = side
			}
			// Inspect before node startup can rebuild files or execute downloaded blocks.
			inspectCrashRecoveryDataDir(t, request.DataDir, func(db kv.TemporalRoDB) {
				assertSnapshotCrashState(t, db, want.state)
				counts := snapshotCrashTableCounts(t, db)
				t.Logf("step-0 MDBX copies: before %v, recovered %v", canonicalCounts, counts)
				for table, count := range counts {
					require.LessOrEqual(t, count, canonicalCounts[table], "%s pruning must not add rows", table)
				}
				if window.pruneCommitted {
					require.True(t, snapshotPruneProgress(canonicalCounts, counts), "pruning must have removed MDBX copies")
				} else {
					require.Equal(t, canonicalCounts, counts, "pruning must not be durable yet")
				}
			})
			args := baseArgs
			args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), request.DataDir
			args.EthConfigTweaker = configureSnapshotCrashRecovery
			eat, err := engineapitester.InitialiseEngineApiTester(t.Context(), args)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, eat.Close()) })
			churn, err := contracts.NewStateChurn(addr, eat.ContractBackend)
			require.NoError(t, err)
			assertChurnState(t.Context(), t, eat, churn, want.payloads[len(want.payloads)-1], want.sum)
			for _, target := range []crashRecoveryChain{prefix, side, canonical, side} {
				insertCrashRecoveryPayloads(t.Context(), t, eat, target.payloads)
				require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), target.payloads[len(target.payloads)-1]))
				assertChurnState(t.Context(), t, eat, churn, target.payloads[len(target.payloads)-1], target.sum)
				assertSnapshotCrashState(t, eat.ChainDB, target.state)
			}
			assertCrashRecoveryContinuation(t, eat, churn, side)
		})
	}
}

func snapshotPruneProgress(before, after map[string]uint64) bool {
	for table, count := range before {
		if after[table] < count {
			return true
		}
	}
	return false
}

func TestSnapshotPruneProgress(t *testing.T) {
	before := map[string]uint64{kv.TblAccountVals: 7, kv.TblStorageVals: 10}
	for _, tc := range []struct {
		name  string
		after map[string]uint64
		want  bool
	}{
		{"none", before, false},
		{"partial", map[string]uint64{kv.TblAccountVals: 5, kv.TblStorageVals: 10}, true},
		{"complete", map[string]uint64{kv.TblAccountVals: 0, kv.TblStorageVals: 0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, snapshotPruneProgress(before, tc.after))
		})
	}
}

func snapshotCrashTableCounts(t *testing.T, db kv.TemporalRoDB) map[string]uint64 {
	t.Helper()
	var counts map[string]uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		counts, err = snapshotCrashTableCountsTx(tx, db.StepSize())
		return err
	}))
	return counts
}

func snapshotCrashTableCountsTx(tx kv.Tx, stepSize uint64) (map[string]uint64, error) {
	counts := make(map[string]uint64)
	// Compare only the first filed step. Reorgs may add tombstones in later
	// steps, so total table sizes need not match a straight-chain reference.
	for _, table := range []string{kv.TblAccountVals, kv.TblStorageVals, kv.TblCodeVals, kv.TblAccountHistoryKeys, kv.TblStorageHistoryKeys} {
		counts[table] = 0
		if err := tx.ForEach(table, nil, func(key, value []byte) error {
			var firstStep bool
			switch table {
			case kv.TblCodeVals:
				if len(key) < 8 {
					return fmt.Errorf("%s: key %x has length %d, want at least 8", table, key, len(key))
				}
				firstStep = ^binary.BigEndian.Uint64(key[len(key)-8:]) == 0
			case kv.TblAccountVals, kv.TblStorageVals:
				if len(value) < 8 {
					return fmt.Errorf("%s: value for key %x has length %d, want at least 8", table, key, len(value))
				}
				firstStep = ^binary.BigEndian.Uint64(value[:8]) == 0
			default:
				if len(key) < 8 {
					return fmt.Errorf("%s: key %x has length %d, want at least 8", table, key, len(key))
				}
				firstStep = binary.BigEndian.Uint64(key) < stepSize
			}
			if firstStep {
				counts[table]++
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return counts, nil
}

func TestSnapshotCrashTableCountsRejectMalformedRecords(t *testing.T) {
	for _, table := range []string{kv.TblAccountVals, kv.TblStorageVals, kv.TblCodeVals, kv.TblAccountHistoryKeys, kv.TblStorageHistoryKeys} {
		t.Run(table, func(t *testing.T) {
			_, tx := mdbxtest.NewTestTx(t)
			require.NoError(t, tx.Put(table, []byte{1}, []byte{1}))
			var err error
			require.NotPanics(t, func() {
				_, err = snapshotCrashTableCountsTx(tx, 32)
			})
			require.ErrorContains(t, err, table)
			require.ErrorContains(t, err, "length 1, want at least 8")
		})
	}
}

func assertSnapshotCrashState(t *testing.T, db kv.TemporalRoDB, want crashRecoveryState) {
	t.Helper()
	require.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		for _, domain := range kv.StateDomains {
			require.NotEmpty(t, tx.Debug().DomainFiles(domain), "recovery must read %s files", domain)
		}
		assertEquivalentCrashRecoveryState(t, want, readCrashRecoveryTx(t, tx))
		return nil
	}))
}

func runSnapshotCrashChild(t *testing.T) {
	var request snapshotCrashRequest
	require.NoError(t, json.NewDecoder(os.Stdin).Decode(&request))
	ctx, cancel := context.WithDeadline(t.Context(), request.Deadline)
	defer cancel()
	key, err := crypto.ToECDSA(request.CoinbaseKey)
	require.NoError(t, err)
	transitions := newStateTransitionController()
	clientTimeout := time.Until(request.Deadline)
	require.Positive(t, clientTimeout)
	buildStarted, buildAllowed, crashReached := make(chan struct{}), make(chan struct{}), make(chan struct{})
	observerFailure, failObserver := context.WithCancelCause(t.Context())
	defer failObserver(nil)
	var firstBuild, firstCrash sync.Once
	var pruneArmed, pruneProgress atomic.Bool
	var filesDone <-chan struct{}
	var beforePrune map[string]uint64
	var stepSize uint64
	var eat engineapitester.EngineApiTester
	pruneWindow := request.Boundary == state.MaintenancePruneReady || request.Boundary == state.MaintenancePruneCommitted
	observe := func(observerCtx context.Context, event state.MaintenanceEvent) {
		point := event.Point
		if point == state.MaintenanceBuildStarted {
			firstBuild.Do(func() {
				filesDone = event.FilesDone
				close(buildStarted)
				select {
				case <-buildAllowed:
				case <-observerCtx.Done():
				}
			})
		}
		if pruneWindow {
			if !pruneArmed.Load() {
				return
			}
			if point == state.MaintenancePruneReady {
				counts, err := snapshotCrashTableCountsTx(event.PruneTx, stepSize)
				if err != nil {
					failObserver(err)
					return
				}
				pruneProgress.Store(snapshotPruneProgress(beforePrune, counts))
			}
			if !pruneProgress.Load() {
				return
			}
		}
		if point != request.Boundary {
			return
		}
		firstCrash.Do(func() {
			if err := observerCtx.Err(); err != nil {
				failObserver(fmt.Errorf("maintenance boundary %d has a cancelled context: %w", point, err))
				return
			}
			if point == state.MaintenanceFilesBuilt || point == state.MaintenanceFilesPublished {
				err := eat.ChainDB.ViewTemporal(observerCtx, func(tx kv.TemporalTx) error {
					for _, domain := range kv.StateDomains {
						visible := len(tx.Debug().DomainFiles(domain)) > 0
						published := point == state.MaintenanceFilesPublished
						if visible != published {
							return fmt.Errorf("%s files visible at maintenance boundary %d: got %t, want %t", domain, point, visible, published)
						}
					}
					return nil
				})
				if err != nil {
					failObserver(err)
					return
				}
			}
			close(crashReached)
			<-observerCtx.Done()
		})
	}
	// Keep the node alive until process termination. An attempt timeout must
	// not release a pre-commit barrier and let persistence continue.
	eat, err = engineapitester.InitialiseEngineApiTester(context.Background(), engineapitester.EngineApiTesterInitArgs{
		Logger: testlog.Logger(t, log.LvlError), DataDir: request.DataDir,
		Genesis: request.Genesis, CoinbaseKey: key, NoEmptyBlock1: true,
		EngineApiClientTimeout: &clientTimeout, StateTransitionObserver: transitions.observe,
		EthConfigTweaker: configureSnapshotCrashRecovery, MaintenanceObserver: observe,
	})
	require.NoError(t, err)
	t.Log("importing canonical blocks with the first file build held")
	for _, payload := range request.Canonical {
		insertCrashRecoveryPayloads(ctx, t, eat, []*engineapitester.MockClPayload{payload})
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))
	}
	select {
	case <-buildStarted:
	case <-ctx.Done():
		t.Fatalf("file building did not start: %v", ctx.Err())
	}
	require.NotNil(t, filesDone, "the held build must identify its completion signal")
	if pruneWindow {
		beforePrune = snapshotCrashTableCounts(t, eat.ChainDB)
		stepSize = eat.ChainDB.StepSize()
	}
	insertCrashRecoveryPayloads(ctx, t, eat, request.Replacement)
	t.Log("holding the reorg after unwind")
	unwound := transitions.hold(t, execmodule.StateTransitionUnwindComplete, 1)
	// A failure must exit before cleanup releases an in-flight crash boundary.
	t.Cleanup(func() { os.Exit(crashRecoveryFailureExitCode) })
	response := make(chan error, 1)
	go func() { response <- eat.MockCl.UpdateForkChoice(ctx, request.Replacement[len(request.Replacement)-1]) }()
	require.NoError(t, waitCrashRecoveryTransition(ctx, unwound, response, false))
	close(buildAllowed)
	t.Logf("waiting for maintenance boundary %d", request.Boundary)
	if pruneWindow {
		select {
		case <-filesDone:
		case <-ctx.Done():
			t.Fatalf("file building did not finish: %v", ctx.Err())
		}
		require.NoError(t, ctx.Err())
		require.Positive(t, eat.StateAgg.EndTxNumMinimax())
		pruneArmed.Store(true)
		unwound.release()
	}
	pruneError := make(chan error, 1)
	waiting := true
	for waiting {
		select {
		case <-crashReached:
			waiting = false
		case err := <-response:
			require.NoError(t, err, "replacement FCU before crash boundary")
			require.True(t, pruneWindow, "file publication must leave the reorg held")
			response = nil
			go func() { pruneError <- continueSnapshotPruning(ctx, eat.ExecutionModule.Prune) }()
		case err := <-pruneError:
			t.Fatalf("pruning ended before the crash boundary: %v", err)
		case <-observerFailure.Done():
			t.Fatalf("maintenance observer: %v", context.Cause(observerFailure))
		case <-ctx.Done():
			t.Fatalf("snapshot crash boundary %d was not reached: %v", request.Boundary, ctx.Err())
		}
	}
	waitForCrashKill(t, ctx, request.ControlAddress, request.Deadline, int(request.Boundary))
}

// A time-budgeted FCU pass may leave all state pruning for a later pass.
// Repeat the same commit/build path while execution is idle until pruning makes progress.
func continueSnapshotPruning(ctx context.Context, prune func(context.Context) (<-chan struct{}, error)) error {
	for ctx.Err() == nil {
		finished, err := prune(ctx)
		if err != nil {
			return err
		}
		select {
		case <-finished:
		case <-ctx.Done():
			return ctx.Err()
		}
		// A pass that starts no build returns an already-closed signal.
		if err := common.Sleep(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func TestSnapshotPruneRetryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		finished := make(chan struct{})
		close(finished)
		calls := 0
		started := time.Now()
		err := continueSnapshotPruning(ctx, func(context.Context) (<-chan struct{}, error) {
			calls++
			if calls == 2 {
				cancel()
			}
			return finished, nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, calls)
		require.Positive(t, time.Since(started), "completed jobs must not cause an immediate retry")
	})
}
