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

package caplin1

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestPrepareForkChoiceDirectoryResumesAfterEveryBoundary(t *testing.T) {
	root := common.HexToHash("0x1234")
	rootHex := fmt.Sprintf("%x", root)
	filename := rootHex + ".envelope.snappy_ssz"
	marker := rootHex + ".envelope.indices-pending"
	boundaries := []string{
		"preserve " + filename,
		"preserve " + marker,
		"clear transient forkchoice",
		"restore " + filename,
		"restore " + marker,
		"remove recovery directory",
	}
	for _, boundary := range boundaries {
		t.Run(boundary, func(t *testing.T) {
			cfg := &clparams.MainnetBeaconConfig
			anchorState := state.New(cfg)
			forkChoicePath := t.TempDir() + "/caplin-forkchoice"
			require.NoError(t, prepareForkChoiceDirectory(forkChoicePath))
			fs := afero.NewBasePathFs(afero.NewOsFs(), forkChoicePath)
			graph := fork_graph.NewForkGraphDisk(anchorState, nil, fs, beacon_router_configuration.RouterConfiguration{})
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			envelope.Message.BeaconBlockRoot = root
			envelope.Message.Payload.BlockHash = common.HexToHash("0xabcd")
			envelope.Message.Payload.BlockNumber = 42
			require.NoError(t, graph.DumpEnvelopeOnDisk(root, envelope))

			errInterrupted := errors.New("interrupted")
			err := prepareForkChoiceDirectoryWithHook(forkChoicePath, func(current string) error {
				if current == boundary {
					return errInterrupted
				}
				return nil
			})
			require.ErrorIs(t, err, errInterrupted)
			require.NoError(t, prepareForkChoiceDirectory(forkChoicePath))

			restartedFS := afero.NewBasePathFs(afero.NewOsFs(), forkChoicePath)
			graph = fork_graph.NewForkGraphDisk(anchorState, nil, restartedFS, beacon_router_configuration.RouterConfiguration{})
			db := memdb.NewTestDB(t, dbcfg.ChainDB)
			_, err = forkchoice.NewForkChoiceStore(
				nil,
				anchorState,
				nil,
				pool.NewOperationsPool(cfg),
				graph,
				beaconevents.NewEventEmitter(),
				synced_data.NewSyncedDataManager(cfg, true),
				nil,
				public_keys_registry.NewInMemoryPublicKeysRegistry(),
				validator_params.NewValidatorParams(),
				false,
				db,
			)
			require.NoError(t, err)
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
				require.NoError(t, err)
				require.Equal(t, uint64(42), *blockNumber)
				return nil
			}))
		})
	}
}

func TestPrepareForkChoiceDirectoryFailsClosedOnArtifactConflict(t *testing.T) {
	root := common.HexToHash("0x1234")
	rootHex := fmt.Sprintf("%x", root)
	filename := rootHex + ".envelope.snappy_ssz"
	marker := rootHex + ".envelope.indices-pending"
	baseDir := t.TempDir()
	forkChoicePath := baseDir + "/caplin-forkchoice"
	recoveryPath := forkChoicePath + "-envelope-recovery"
	require.NoError(t, os.MkdirAll(forkChoicePath, 0o755))
	require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(forkChoicePath, filename), []byte("source"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(recoveryPath, filename), []byte("recovery"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(recoveryPath, marker), nil, 0o644))

	err := prepareForkChoiceDirectory(forkChoicePath)
	require.ErrorContains(t, err, "already exists")
	source, readErr := os.ReadFile(filepath.Join(forkChoicePath, filename))
	require.NoError(t, readErr)
	require.Equal(t, []byte("source"), source)
}

func TestPrepareForkChoiceDirectoryResumesTemporaryEnvelopeMoves(t *testing.T) {
	for _, phase := range []string{"preserve ", "restore "} {
		t.Run(phase, func(t *testing.T) {
			cfg := &clparams.MainnetBeaconConfig
			anchorState := state.New(cfg)
			forkChoicePath := t.TempDir() + "/caplin-forkchoice"
			require.NoError(t, prepareForkChoiceDirectory(forkChoicePath))
			fs := afero.NewBasePathFs(afero.NewOsFs(), forkChoicePath)
			graph := fork_graph.NewForkGraphDisk(anchorState, nil, fs, beacon_router_configuration.RouterConfiguration{})
			persistence := graph.(fork_graph.EnvelopePersistence)
			root := common.HexToHash("0x9999")
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			envelope.Message.BeaconBlockRoot = root
			_, err := persistence.PrepareEnvelopeOnDisk(root, envelope, false)
			require.NoError(t, err)
			rootHex := fmt.Sprintf("%x", root)
			temporaryFiles, err := filepath.Glob(filepath.Join(forkChoicePath, rootHex+".envelope.snappy_ssz.tmp-*"))
			require.NoError(t, err)
			require.Len(t, temporaryFiles, 1)
			boundary := phase + filepath.Base(temporaryFiles[0])

			errInterrupted := errors.New("interrupted")
			err = prepareForkChoiceDirectoryWithHook(forkChoicePath, func(current string) error {
				if current == boundary {
					return errInterrupted
				}
				return nil
			})
			require.ErrorIs(t, err, errInterrupted)
			require.NoError(t, prepareForkChoiceDirectory(forkChoicePath))

			restartedFS := afero.NewBasePathFs(afero.NewOsFs(), forkChoicePath)
			graph = fork_graph.NewForkGraphDisk(anchorState, nil, restartedFS, beacon_router_configuration.RouterConfiguration{})
			persistence = graph.(fork_graph.EnvelopePersistence)
			db := memdb.NewTestDB(t, dbcfg.ChainDB)
			_, err = forkchoice.NewForkChoiceStore(
				nil,
				anchorState,
				nil,
				pool.NewOperationsPool(cfg),
				graph,
				beaconevents.NewEventEmitter(),
				synced_data.NewSyncedDataManager(cfg, true),
				nil,
				public_keys_registry.NewInMemoryPublicKeysRegistry(),
				validator_params.NewValidatorParams(),
				false,
				db,
			)
			require.NoError(t, err)
			pendingRoots, err := persistence.PendingEnvelopeIndexRoots()
			require.NoError(t, err)
			require.NotContains(t, pendingRoots, root)
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
				require.NoError(t, err)
				require.Nil(t, blockNumber)
				return nil
			}))
			temporaryFiles, err = filepath.Glob(filepath.Join(forkChoicePath, rootHex+".envelope.snappy_ssz.tmp-*"))
			require.NoError(t, err)
			require.Empty(t, temporaryFiles)
		})
	}
}

func TestPrepareForkChoiceDirectoryPreservesPendingIndexRecovery(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	anchorState := state.New(cfg)
	baseDir := t.TempDir()
	forkChoicePath := baseDir + "/caplin-forkchoice"
	require.NoError(t, prepareForkChoiceDirectory(forkChoicePath))
	fs := afero.NewBasePathFs(afero.NewOsFs(), forkChoicePath)
	graph := fork_graph.NewForkGraphDisk(anchorState, nil, fs, beacon_router_configuration.RouterConfiguration{})
	persistence := graph.(fork_graph.EnvelopePersistence)

	pendingRoot := common.HexToHash("0x1234")
	pendingEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	pendingEnvelope.Message.BeaconBlockRoot = pendingRoot
	pendingEnvelope.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	pendingEnvelope.Message.Payload.BlockNumber = 42
	require.NoError(t, graph.DumpEnvelopeOnDisk(pendingRoot, pendingEnvelope))

	committedRoot := common.HexToHash("0x5678")
	committedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	committedEnvelope.Message.BeaconBlockRoot = committedRoot
	committedEnvelope.Message.Payload.BlockHash = common.HexToHash("0xcdef")
	committedEnvelope.Message.Payload.BlockNumber = 43
	require.NoError(t, graph.DumpEnvelopeOnDisk(committedRoot, committedEnvelope))

	orphanRoot := common.HexToHash("0x9999")
	orphanEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	orphanEnvelope.Message.BeaconBlockRoot = orphanRoot
	_, err := persistence.PrepareEnvelopeOnDisk(orphanRoot, orphanEnvelope, false)
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, committedRoot, committedEnvelope.Message)
	}))
	require.NoError(t, prepareForkChoiceDirectory(forkChoicePath))
	restartedFS := afero.NewBasePathFs(afero.NewOsFs(), forkChoicePath)
	graph = fork_graph.NewForkGraphDisk(anchorState, nil, restartedFS, beacon_router_configuration.RouterConfiguration{})
	persistence = graph.(fork_graph.EnvelopePersistence)

	_, err = forkchoice.NewForkChoiceStore(
		nil,
		anchorState,
		nil,
		pool.NewOperationsPool(cfg),
		graph,
		beaconevents.NewEventEmitter(),
		synced_data.NewSyncedDataManager(cfg, true),
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		db,
	)
	require.NoError(t, err)

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		pendingNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, pendingRoot)
		require.NoError(t, err)
		require.Equal(t, uint64(42), *pendingNumber)
		committedNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, committedRoot)
		require.NoError(t, err)
		require.Equal(t, uint64(43), *committedNumber)
		orphanNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, orphanRoot)
		require.NoError(t, err)
		require.Nil(t, orphanNumber)
		return nil
	}))
	pendingRoots, err := persistence.PendingEnvelopeIndexRoots()
	require.NoError(t, err)
	require.Empty(t, pendingRoots)
}
