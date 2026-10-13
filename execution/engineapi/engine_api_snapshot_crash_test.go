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
	"encoding/hex"
	"encoding/json"
	"maps"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

const snapshotCrashChild = "ERIGON_SNAPSHOT_CRASH_CHILD"

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
	config.MaxReorgDepth = 32
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
		{"files_built", state.MaintenanceFilesBuilt, false, false},
		{"files_published", state.MaintenanceFilesPublished, false, false},
		{"prune_ready", state.MaintenancePruneReady, true, false},
		{"prune_committed", state.MaintenancePruneCommitted, true, true},
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
				for table, count := range snapshotCrashTableCounts(t, db) {
					if window.pruneCommitted {
						require.Less(t, count, canonicalCounts[table], "%s must have lost MDBX copies", table)
					} else {
						require.Equal(t, canonicalCounts[table], count, "%s pruning must not be durable yet", table)
					}
				}
			})
			args := baseArgs
			args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), request.DataDir
			args.EthConfigTweaker = configureSnapshotCrashRecovery
			lastSlot := side.payloads[len(side.payloads)-1].ExecutionPayload.SlotNumber
			require.NotNil(t, lastSlot)
			args.MockClState = &engineapitester.MockClState{SlotNumber: uint64(*lastSlot) + 1}
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

func snapshotCrashTableCounts(t *testing.T, db kv.TemporalRoDB) map[string]uint64 {
	t.Helper()
	counts := make(map[string]uint64)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		// Compare only the first filed step. Reorgs may add tombstones in later
		// steps, so total table sizes need not match a straight-chain reference.
		for _, table := range []string{kv.TblAccountVals, kv.TblStorageVals, kv.TblCodeVals, kv.TblAccountHistoryKeys, kv.TblStorageHistoryKeys} {
			counts[table] = 0
			if err := tx.ForEach(table, nil, func(key, value []byte) error {
				var firstStep bool
				switch table {
				case kv.TblCodeVals:
					firstStep = ^binary.BigEndian.Uint64(key[len(key)-8:]) == 0
				case kv.TblAccountVals, kv.TblStorageVals:
					firstStep = ^binary.BigEndian.Uint64(value[:8]) == 0
				default:
					firstStep = binary.BigEndian.Uint64(key) < db.StepSize()
				}
				if firstStep {
					counts[table]++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}))
	return counts
}

func assertSnapshotCrashState(t *testing.T, db kv.TemporalRoDB, want crashRecoveryState) {
	t.Helper()
	require.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain} {
			require.NotEmpty(t, tx.Debug().DomainFiles(domain), "recovery must read %s files", domain)
		}
		got := readCrashRecoveryTx(t, tx)
		want.Domains = maps.Clone(want.Domains)
		want.Domains[kv.CommitmentDomain] = snapshotCrashCommitment(t, want.Domains[kv.CommitmentDomain])
		got.Domains[kv.CommitmentDomain] = snapshotCrashCommitment(t, got.Domains[kv.CommitmentDomain])
		assertCrashRecoveryState(t, want, got)
		return nil
	}))
}

func snapshotCrashCommitment(t *testing.T, entries map[string]string) map[string]string {
	t.Helper()
	entries = maps.Clone(entries)
	for key, value := range entries {
		if key == hex.EncodeToString(commitmentdb.KeyCommitmentState) {
			continue
		}
		encoded, err := hex.DecodeString(value)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(encoded), 4, "commitment branch %s", key)
		// Empty branches can be deletion records or absent after unwind/compaction.
		if len(encoded) == 4 && commitment.BranchData(encoded).ChildCount() == 0 {
			delete(entries, key)
		}
	}
	return entries
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
	eat, err := engineapitester.InitialiseEngineApiTester(context.Background(), engineapitester.EngineApiTesterInitArgs{
		Logger: testlog.Logger(t, log.LvlError), DataDir: request.DataDir,
		Genesis: request.Genesis, CoinbaseKey: key, NoEmptyBlock1: true,
		EngineApiClientTimeout: &clientTimeout, StateTransitionObserver: transitions.observe,
		EthConfigTweaker: configureSnapshotCrashRecovery,
	})
	require.NoError(t, err)
	buildStarted, buildAllowed, crashReached := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstBuild, firstCrash sync.Once
	var pruneArmed atomic.Bool
	pruneWindow := request.Boundary == state.MaintenancePruneReady || request.Boundary == state.MaintenancePruneCommitted
	eat.StateAgg.SetMaintenanceObserver(func(ctx context.Context, point state.MaintenancePoint) {
		if point == state.MaintenanceBuildStarted {
			firstBuild.Do(func() {
				close(buildStarted)
				select {
				case <-buildAllowed:
				case <-ctx.Done():
				}
			})
		}
		if point != request.Boundary || pruneWindow && !pruneArmed.Load() {
			return
		}
		firstCrash.Do(func() {
			close(crashReached)
			<-ctx.Done()
		})
	})
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
		case <-eat.StateAgg.WaitForBuildAndMerge(ctx):
		case <-ctx.Done():
			t.Fatalf("file building did not finish: %v", ctx.Err())
		}
		require.NoError(t, ctx.Err())
		require.Positive(t, eat.StateAgg.EndTxNumMinimax())
		pruneArmed.Store(true)
		unwound.release()
	}
	waiting := true
	for waiting {
		select {
		case <-crashReached:
			waiting = false
		case err := <-response:
			require.NoError(t, err, "replacement FCU before crash boundary")
			require.True(t, pruneWindow, "file publication must leave the reorg held")
			response = nil
		case <-ctx.Done():
			t.Fatalf("snapshot crash boundary %d was not reached: %v", request.Boundary, ctx.Err())
		}
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", request.ControlAddress)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetWriteDeadline(request.Deadline))
	require.NoError(t, json.NewEncoder(conn).Encode(request.Boundary))
	<-ctx.Done()
	t.Fatalf("parent did not kill the child: %v", ctx.Err())
}
