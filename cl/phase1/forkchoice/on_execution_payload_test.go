// Copyright 2024 The Erigon Authors
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

package forkchoice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

type failFirstUpdateDB struct {
	kv.RwDB
	updates atomic.Int32
}

type ownerAdmissionContext struct {
	context.Context
	observed  chan struct{}
	observeAt int32
	calls     atomic.Int32
	once      sync.Once
}

func (c *ownerAdmissionContext) Done() <-chan struct{} {
	if c.calls.Add(1) == c.observeAt {
		c.once.Do(func() { close(c.observed) })
	}
	return c.Context.Done()
}

func (db *failFirstUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if db.updates.Add(1) == 1 {
		return errors.New("injected index failure")
	}
	return db.RwDB.Update(ctx, f)
}

// TestValidateEnvelopeAgainstBlock_NoBid tests that validation fails when block has no bid
func TestValidateEnvelopeAgainstBlock_NoBid(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.SlotNumber = 100 // Must match block.Slot to pass slot_number check
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload:      payload,
		},
	}

	// Block without bid (SignedExecutionPayloadBid is nil by default)
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = nil // Explicitly set to nil

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block missing signed_execution_payload_bid")
}

// TestValidateEnvelopeAgainstBlock_SlotNumberMismatch tests that validation fails when
// block.slot != envelope.payload.slot_number (EIP-7843 / GLOAS p2p-interface REJECT rule).
func TestValidateEnvelopeAgainstBlock_SlotNumberMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	blockHash := common.HexToHash("0x1234")
	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.BlockHash = blockHash
	payload.SlotNumber = 200 // Different from block slot

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload:      payload,
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          blockHash,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100, // Different from payload.SlotNumber
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block slot 100 != envelope.payload.slot_number 200")
}

// TestValidateEnvelopeAgainstBlock_BuilderIndexMismatch tests that validation fails when builder indices don't match
func TestValidateEnvelopeAgainstBlock_BuilderIndexMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	blockHash := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload: &cltypes.Eth1Block{
				BlockHash:  blockHash,
				SlotNumber: 100, // Match block.Slot to pass slot_number check
			},
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       2, // Different builder
			BlockHash:          blockHash,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "envelope builder_index 1 != bid builder_index 2")
}

// TestValidateEnvelopeAgainstBlock_NilPayload tests that validation fails when envelope has no payload
func TestValidateEnvelopeAgainstBlock_NilPayload(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload:      nil, // No payload
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          common.HexToHash("0x1234"),
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "envelope missing payload")
}

// TestValidateEnvelopeAgainstBlock_BlockHashMismatch tests that validation fails when block hashes don't match
func TestValidateEnvelopeAgainstBlock_BlockHashMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload: &cltypes.Eth1Block{
				BlockHash:  common.HexToHash("0x1111"), // Different hash
				SlotNumber: 100,                        // Match block.Slot
			},
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          common.HexToHash("0x2222"), // Different hash
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload block_hash")
	require.Contains(t, err.Error(), "!= bid block_hash")
}

// TestCheckDataAvailability_NoBid tests that checkDataAvailability returns nil when there's no bid
func TestCheckDataAvailability_NoBid(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = nil

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.checkDataAvailability(context.TODO(), block, common.Hash{})
	require.NoError(t, err)
}

// TestCheckDataAvailability_NoBlobs tests that checkDataAvailability returns nil when there are no blobs
func TestCheckDataAvailability_NoBlobs(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          common.HexToHash("0x1234"),
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48), // Empty
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.checkDataAvailability(context.TODO(), block, common.Hash{})
	require.NoError(t, err)
}

// TestValidatePayloadWithEL_NoEngine tests that payload validation returns nil when there's no engine.
func TestValidatePayloadWithEL_NoEngine(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{
		beaconCfg: cfg,
		engine:    nil, // No engine
	}

	envelope := &cltypes.ExecutionPayloadEnvelope{
		Payload: cltypes.NewEth1Block(clparams.GloasVersion, cfg),
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	_, err := f.validatePayloadWithELLocked(context.TODO(), envelope, block, common.Hash{})
	require.NoError(t, err)
}

func TestOnExecutionPayloadRepairsIndicesAfterPriorWriteFailure(t *testing.T) {
	root := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockHash = executionHash
	envelope.Message.Payload.BlockNumber = 42

	db := &failFirstUpdateDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB)}
	pending, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		forkGraph:        payloadVoteForkGraph{hasEnvelope: true, envelope: envelope},
		db:               db,
		pendingEnvelopes: pending,
	}

	require.ErrorContains(t, f.OnExecutionPayload(t.Context(), envelope, false, false), "injected index failure")
	require.Equal(t, 1, pending.Len())
	require.NoError(t, f.OnExecutionPayload(t.Context(), envelope, false, false))

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
		require.NoError(t, err)
		require.NotNil(t, blockNumber)
		require.Equal(t, uint64(42), *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, executionHash, blockHash)
		return nil
	}))
}

func TestApplyLocalSelfBuildEnvelopeRepairsIndicesAfterPriorWriteFailure(t *testing.T) {
	root := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockHash = executionHash
	envelope.Message.Payload.BlockNumber = 42

	db := &failFirstUpdateDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB)}
	pending, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		forkGraph:                      payloadVoteForkGraph{hasEnvelope: true, envelope: envelope},
		db:                             db,
		pendingLocalSelfBuildEnvelopes: pending,
	}

	require.ErrorContains(t, f.ApplyLocalSelfBuildEnvelope(t.Context(), envelope), "injected index failure")
	require.Equal(t, 1, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	require.Zero(t, pending.Len())

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
		require.NoError(t, err)
		require.NotNil(t, blockNumber)
		require.Equal(t, uint64(42), *blockNumber)
		return nil
	}))
}

func TestReconcilePendingEnvelopeIndicesAfterRestart(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	anchorState := state.New(cfg)
	baseDir := t.TempDir()
	fs := afero.NewBasePathFs(afero.NewOsFs(), baseDir)
	graph := fork_graph.NewForkGraphDisk(anchorState, nil, fs, beacon_router_configuration.RouterConfiguration{})
	persistence := graph.(fork_graph.EnvelopePersistence)
	root := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockHash = executionHash
	envelope.Message.Payload.BlockNumber = 42
	require.NoError(t, graph.DumpEnvelopeOnDisk(root, envelope))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	committedRoot := common.HexToHash("0x5678")
	committedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	committedEnvelope.Message.BeaconBlockRoot = committedRoot
	committedEnvelope.Message.Payload.BlockHash = common.HexToHash("0xcdef")
	committedEnvelope.Message.Payload.BlockNumber = 43
	require.NoError(t, graph.DumpEnvelopeOnDisk(committedRoot, committedEnvelope))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, committedRoot, committedEnvelope.Message)
	}))
	orphanRoot := common.HexToHash("0x9999")
	orphanEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	orphanEnvelope.Message.BeaconBlockRoot = orphanRoot
	_, err := persistence.PrepareEnvelopeOnDisk(orphanRoot, orphanEnvelope, false)
	require.NoError(t, err)

	restartedFS := afero.NewBasePathFs(afero.NewOsFs(), baseDir)
	graph = fork_graph.NewForkGraphDisk(anchorState, nil, restartedFS, beacon_router_configuration.RouterConfiguration{})
	persistence = graph.(fork_graph.EnvelopePersistence)

	_, err = NewForkChoiceStore(
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
		require.NotNil(t, blockNumber)
		require.Equal(t, uint64(42), *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, executionHash, blockHash)
		committedNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, committedRoot)
		require.NoError(t, err)
		require.NotNil(t, committedNumber)
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

func TestApplyLocalSelfBuildDoesNotPromoteELValidCLInvalidEnvelope(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	blockState := state.New(cfg)
	blockState.SetVersion(clparams.GloasVersion)
	blockState.SetSlot(1)
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BlockHash:          executionHash,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1, Body: body}}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockHash = executionHash
	envelope.Message.Payload.SlotNumber = 1

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(execution_client.PayloadStatusValidated, nil)
	verified, err := lru.New[common.Hash, struct{}](16)
	require.NoError(t, err)
	executionStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	statusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	gasLimits, err := lru.New[common.Hash, uint64](16)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                payloadVoteForkGraph{block: block, blockState: blockState},
		verifiedExecutionPayload: verified,
		executionPayloadStatus:   executionStatus,
		payloadStatusByRoot:      statusByRoot,
		executionPayloadGasLimit: gasLimits,
		eth2Roots:                eth2Roots,
		optimisticStore:          optimistic.NewOptimisticStore(),
	}

	err = f.ApplyLocalSelfBuildEnvelope(t.Context(), envelope)
	require.ErrorContains(t, err, "beacon_block_root")
	require.False(t, f.IsPayloadVerified(root))
	status, ok := f.GetRecentExecutionPayloadStatusByRoot(root)
	require.False(t, ok)
	require.Zero(t, status)
}

func TestOnExecutionPayloadValidatesPersistedEnvelopeBeforeAcceptingRecovery(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	blockState := state.New(cfg)
	blockState.SetVersion(clparams.GloasVersion)
	blockState.SetSlot(1)
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BlockHash:          executionHash,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1, Body: body}}
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	persisted.Message.BeaconBlockRoot = root
	persisted.Message.Payload.BlockHash = executionHash
	persisted.Message.Payload.SlotNumber = 1
	requestsRoot, err := persisted.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	body.SignedExecutionPayloadBid.Message.ExecutionRequestsRoot = requestsRoot

	f := &ForkChoiceStore{
		beaconCfg: cfg,
		forkGraph: payloadVoteForkGraph{
			hasEnvelope: true,
			envelope:    persisted,
			block:       block,
			blockState:  blockState,
		},
	}

	err = f.OnExecutionPayload(t.Context(), persisted, false, true)
	require.ErrorContains(t, err, "invalid builder signature")
}

func TestOnExecutionPayloadRejectsCallerDifferentFromPersistedEnvelope(t *testing.T) {
	root := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = root
	caller := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	caller.Message.BeaconBlockRoot = root
	caller.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	f := &ForkChoiceStore{forkGraph: payloadVoteForkGraph{hasEnvelope: true, envelope: persisted}}

	err := f.OnExecutionPayload(t.Context(), caller, false, true)
	require.ErrorContains(t, err, "does not match persisted envelope")
}

func TestOnExecutionPayloadRejectsIncompleteCallerBeforePersistedFallback(t *testing.T) {
	root := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = root
	caller := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	caller.Message.BeaconBlockRoot = root
	caller.Message.Payload = nil
	f := &ForkChoiceStore{forkGraph: payloadVoteForkGraph{hasEnvelope: true, envelope: persisted}}

	err := f.OnExecutionPayload(t.Context(), caller, false, true)
	require.ErrorContains(t, err, "incomplete execution payload envelope")
}

func TestOnExecutionPayloadReplacesLocalSignatureWithAuthenticatedEnvelope(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	blockState := state.New(&cfg)
	blockState.SetVersion(clparams.GloasVersion)
	blockState.SetSlot(1)
	parentRoot := common.HexToHash("0x2222")
	stateRoot := common.HexToHash("0x1111")
	header := &cltypes.BeaconBlockHeader{Slot: 1, ParentRoot: parentRoot, ProposerIndex: 0}
	blockState.SetLatestBlockHeader(header)
	blockState.SetPreviousStateRoot(stateRoot)
	headerWithStateRoot := *header
	headerWithStateRoot.Root = stateRoot
	blockRoot, err := headerWithStateRoot.HashSSZ()
	require.NoError(t, err)

	privateKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	pubkey := common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey()))
	blockState.AddValidator(solid.NewValidatorFromParameters(pubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.ParentBeaconBlockRoot = parentRoot
	envelope.Message.Payload.SlotNumber = 1
	envelope.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	envelope.Message.Payload.ParentHash = common.HexToHash("0x3333")
	envelope.Message.Payload.Time = cfg.SecondsPerSlot
	envelope.Message.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	requestsRoot, err := envelope.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	bid := &cltypes.ExecutionPayloadBid{
		BuilderIndex:          envelope.Message.BuilderIndex,
		PrevRandao:            envelope.Message.Payload.PrevRandao,
		GasLimit:              envelope.Message.Payload.GasLimit,
		BlockHash:             envelope.Message.Payload.BlockHash,
		ExecutionRequestsRoot: requestsRoot,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
	}
	blockState.SetLatestExecutionPayloadBid(bid)
	blockState.SetLatestBlockHash(envelope.Message.Payload.ParentHash)
	blockState.SetPayloadExpectedWithdrawals(envelope.Message.Payload.Withdrawals)
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: bid, Signature: common.Bytes96(bls.InfiniteSignature)}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1, ParentRoot: parentRoot, StateRoot: stateRoot, Body: body}}
	domain, err := blockState.GetDomain(cfg.DomainBeaconBuilder, state.GetEpochAtSlot(&cfg, 1))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(envelope.Message, domain)
	require.NoError(t, err)
	copy(envelope.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	localEnvelope := envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	localEnvelope.Signature = common.Bytes96(bls.InfiniteSignature)
	var preparedEnvelope *cltypes.SignedExecutionPayloadEnvelope
	var prepareRequiresBlock bool
	eth2Roots, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		beaconCfg: &cfg,
		forkGraph: payloadVoteForkGraph{
			hasEnvelope:          true,
			envelope:             localEnvelope,
			block:                block,
			blockState:           blockState,
			preparedEnvelope:     &preparedEnvelope,
			prepareRequiresBlock: &prepareRequiresBlock,
		},
		eth2Roots: eth2Roots,
	}

	require.NoError(t, f.OnExecutionPayload(t.Context(), envelope, false, true))
	require.Same(t, envelope, preparedEnvelope)
	require.True(t, prepareRequiresBlock)
}

func TestOnExecutionPayloadRejectsInfiniteSignature(t *testing.T) {
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Signature = common.Bytes96(bls.InfiniteSignature)

	err := (&ForkChoiceStore{}).OnExecutionPayload(t.Context(), envelope, false, false)
	require.ErrorContains(t, err, "unauthenticated")
}

func TestPendingLocalSelfBuildEnvelopeSurvivesCanceledApplyAndRetries(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	blockState := state.New(cfg)
	blockState.SetVersion(clparams.GloasVersion)
	blockState.SetSlot(1)
	blockState.SetGenesisTime(0)
	stateRoot := common.HexToHash("0x1111")
	parentRoot := common.HexToHash("0x2222")
	header := &cltypes.BeaconBlockHeader{Slot: 1, ParentRoot: parentRoot}
	blockState.SetLatestBlockHeader(header)
	blockState.SetPreviousStateRoot(stateRoot)
	headerWithStateRoot := *header
	headerWithStateRoot.Root = stateRoot
	blockRoot, err := headerWithStateRoot.HashSSZ()
	require.NoError(t, err)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.ParentBeaconBlockRoot = parentRoot
	envelope.Message.Payload.SlotNumber = 1
	envelope.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	envelope.Message.Payload.ParentHash = common.HexToHash("0x3333")
	envelope.Message.Payload.Time = cfg.SecondsPerSlot
	envelope.Message.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	requestsRoot, err := envelope.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	bid := &cltypes.ExecutionPayloadBid{
		BuilderIndex:          envelope.Message.BuilderIndex,
		PrevRandao:            envelope.Message.Payload.PrevRandao,
		GasLimit:              envelope.Message.Payload.GasLimit,
		BlockHash:             envelope.Message.Payload.BlockHash,
		ExecutionRequestsRoot: requestsRoot,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
	}
	blockState.SetLatestExecutionPayloadBid(bid)
	blockState.SetLatestBlockHash(envelope.Message.Payload.ParentHash)
	blockState.SetPayloadExpectedWithdrawals(envelope.Message.Payload.Withdrawals)
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: bid}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{
		Slot:       1,
		ParentRoot: parentRoot,
		StateRoot:  stateRoot,
		Body:       body,
	}}

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(execution_client.PayloadStatusNone, context.Canceled)
	engine.EXPECT().NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(execution_client.PayloadStatusValidated, nil).Times(2)
	pending, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	verified, err := lru.New[common.Hash, struct{}](16)
	require.NoError(t, err)
	executionStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	statusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	gasLimits, err := lru.New[common.Hash, uint64](16)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)
	dumpedRoot := common.Hash{}
	var persisted atomic.Bool
	var dumpCalls atomic.Int32
	dumpStarted := make(chan struct{})
	releaseDump := make(chan struct{})
	graph := &payloadVoteForkGraph{
		dumpedEnvelope:   &dumpedRoot,
		hasEnvelopeState: &persisted,
		dumpCalls:        &dumpCalls,
		dumpStarted:      dumpStarted,
		releaseDump:      releaseDump,
		dumpErr:          errors.New("injected persistence failure"),
	}
	f := &ForkChoiceStore{
		beaconCfg:                      cfg,
		engine:                         engine,
		forkGraph:                      graph,
		pendingLocalSelfBuildEnvelopes: pending,
		verifiedExecutionPayload:       verified,
		executionPayloadStatus:         executionStatus,
		payloadStatusByRoot:            statusByRoot,
		executionPayloadGasLimit:       gasLimits,
		eth2Roots:                      eth2Roots,
		optimisticStore:                optimistic.NewOptimisticStore(),
	}
	var childObservedHalfPublished atomic.Bool
	graph.onEnvelopePublished = func() {
		if !f.mu.TryLock() {
			return
		}
		defer f.mu.Unlock()
		if _, ok := f.eth2Roots.Peek(common.Hash(blockRoot)); !ok {
			childObservedHalfPublished.Store(true)
		}
	}

	require.ErrorIs(t, f.ApplyLocalSelfBuildEnvelope(t.Context(), envelope), ErrIgnore)
	_, ok := pending.Get(blockRoot)
	require.True(t, ok)
	graph.block = block
	graph.blockState = blockState
	require.Equal(t, 1, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	_, ok = pending.Get(blockRoot)
	require.True(t, ok)
	require.Equal(t, 1, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	require.False(t, f.IsPayloadVerified(blockRoot))
	_, ok = eth2Roots.Get(blockRoot)
	require.False(t, ok)
	_, ok = pending.Get(blockRoot)
	require.True(t, ok)
	firstDone := make(chan int, 1)
	go func() { firstDone <- f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1) }()
	<-dumpStarted

	lockAcquired := make(chan struct{})
	go func() {
		f.mu.Lock()
		close(lockAcquired)
		f.mu.Unlock()
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("forkchoice mutex stayed locked during envelope persistence")
	}

	duplicateDone := make(chan error, 1)
	go func() { duplicateDone <- f.ApplyLocalSelfBuildEnvelope(t.Context(), envelope) }()
	select {
	case err := <-duplicateDone:
		t.Fatalf("same-root retry bypassed persistence ownership: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseDump)
	require.Equal(t, 1, <-firstDone)
	require.NoError(t, <-duplicateDone)
	_, ok = pending.Get(blockRoot)
	require.False(t, ok)
	require.Equal(t, common.Hash(blockRoot), dumpedRoot)
	require.Equal(t, int32(2), dumpCalls.Load())
	require.False(t, childObservedHalfPublished.Load(), "child import observed the envelope before its execution root was promoted")
}

func TestRetryPendingExecutionPayloadEnvelopesIsBoundedAndFair(t *testing.T) {
	gossip, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		pendingEnvelopes:               gossip,
		pendingLocalSelfBuildEnvelopes: local,
	}
	for i := byte(1); i <= 3; i++ {
		gossip.Add(common.Hash{i}, &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now(), envelope: &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{i}},
		}})
		local.Add(common.Hash{i + 3}, &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now(), envelope: &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{i + 3}},
		}})
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.Zero(t, f.RetryPendingExecutionPayloadEnvelopes(canceled, 1))
	require.Equal(t, 1, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	require.Equal(t, 2, gossip.Len())
	require.Equal(t, 3, local.Len())

	require.Equal(t, 3, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 3))
	require.Equal(t, 1, gossip.Len())
	require.Equal(t, 1, local.Len())

	require.Equal(t, 1, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	require.Equal(t, 0, gossip.Len())
	require.Equal(t, 1, local.Len())
}

func TestRetryPendingExecutionPayloadEnvelopesRotatesDeferredWork(t *testing.T) {
	gossip, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		forkGraph:        payloadVoteForkGraph{},
		pendingEnvelopes: gossip,
	}
	for i := byte(1); i <= 3; i++ {
		gossip.Add(common.Hash{i}, &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now(), envelope: &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{
				BeaconBlockRoot:   common.Hash{i},
				Payload:           cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig),
				ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion),
			},
		}})
	}

	require.Equal(t, 2, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 2))
	require.Equal(t, []common.Hash{{3}, {1}, {2}}, gossip.Keys())
	require.Equal(t, 2, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 2))
	require.Equal(t, []common.Hash{{2}, {3}, {1}}, gossip.Keys())
}

func TestRetryPendingExecutionPayloadEnvelopesExpiredWorkDoesNotConsumeLimit(t *testing.T) {
	gossip, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	expiredRoot := common.Hash{1}
	freshRoot := common.Hash{2}
	gossip.Add(expiredRoot, &pendingExecutionPayloadEnvelopeEntry{
		createdAt: time.Now().Add(-pendingExecutionPayloadEnvelopeExpiry - time.Second),
		envelope:  &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: expiredRoot}},
	})
	gossip.Add(freshRoot, &pendingExecutionPayloadEnvelopeEntry{
		createdAt: time.Now(),
		envelope:  &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: freshRoot}},
	})
	f := &ForkChoiceStore{pendingEnvelopes: gossip}

	require.Equal(t, 1, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	require.Zero(t, gossip.Len())
}

func TestPendingExecutionPayloadEnvelopeExpiryWithoutFinalizedCheckpoint(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.Hash{1}
	require.False(t, f.pendingExecutionPayloadEnvelopeExpired(root, &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now()}))
	require.True(t, f.pendingExecutionPayloadEnvelopeExpired(root, &pendingExecutionPayloadEnvelopeEntry{
		createdAt: time.Now().Add(-pendingExecutionPayloadEnvelopeExpiry - time.Second),
	}))
}

func TestPendingExecutionPayloadEnvelopeFinalizedBoundary(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	entry := &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now()}
	f := &ForkChoiceStore{beaconCfg: cfg}
	f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1})
	finalizedSlot := cfg.SlotsPerEpoch

	for _, tc := range []struct {
		name    string
		slot    uint64
		expired bool
	}{
		{name: "before boundary", slot: finalizedSlot - 1, expired: true},
		{name: "exact boundary", slot: finalizedSlot, expired: true},
		{name: "after boundary", slot: finalizedSlot + 1, expired: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.forkGraph = payloadVoteForkGraph{block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: tc.slot}}}
			require.Equal(t, tc.expired, f.pendingExecutionPayloadEnvelopeExpired(common.Hash{1}, entry))
		})
	}
}

func TestRetryPendingExecutionPayloadEnvelopesCancellationWhileOwnerHeld(t *testing.T) {
	gossip, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	root := common.Hash{1}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root
	gossip.Add(root, &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now(), envelope: envelope})
	f := &ForkChoiceStore{pendingEnvelopes: gossip}
	unlocksOwner := f.lockEnvelopeOwner(root)

	baseCtx, cancel := context.WithCancel(t.Context())
	ctx := &ownerAdmissionContext{Context: baseCtx, observed: make(chan struct{}), observeAt: 2}
	retryDone := make(chan int, 1)
	go func() {
		retryDone <- f.RetryPendingExecutionPayloadEnvelopes(ctx, 1)
	}()
	select {
	case <-ctx.observed:
	case <-time.After(time.Second):
		unlocksOwner()
		t.Fatal("retry did not reach envelope owner admission")
	}
	cancel()
	select {
	case attempted := <-retryDone:
		require.Equal(t, 1, attempted)
	case <-time.After(time.Second):
		unlocksOwner()
		t.Fatal("retry ignored cancellation while waiting for envelope owner")
	}
	require.Equal(t, 1, gossip.Len())
	unlocksOwner()
}

func TestRetryPendingExecutionPayloadEnvelopesCancellationWhileAnotherRetryActive(t *testing.T) {
	gossip, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	root := common.Hash{1}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root
	gossip.Add(root, &pendingExecutionPayloadEnvelopeEntry{createdAt: time.Now(), envelope: envelope})
	f := &ForkChoiceStore{pendingEnvelopes: gossip}
	unlocksOwner := f.lockEnvelopeOwner(root)

	firstBaseCtx, cancelFirst := context.WithCancel(t.Context())
	firstCtx := &ownerAdmissionContext{Context: firstBaseCtx, observed: make(chan struct{}), observeAt: 2}
	firstDone := make(chan int, 1)
	go func() { firstDone <- f.RetryPendingExecutionPayloadEnvelopes(firstCtx, 1) }()
	select {
	case <-firstCtx.observed:
	case <-time.After(time.Second):
		unlocksOwner()
		t.Fatal("first retry did not reach envelope owner admission")
	}

	secondBaseCtx, cancelSecond := context.WithCancel(t.Context())
	secondCtx := &ownerAdmissionContext{Context: secondBaseCtx, observed: make(chan struct{}), observeAt: 1}
	secondDone := make(chan int, 1)
	go func() { secondDone <- f.RetryPendingExecutionPayloadEnvelopes(secondCtx, 1) }()
	select {
	case <-secondCtx.observed:
	case <-time.After(time.Second):
		cancelFirst()
		unlocksOwner()
		t.Fatal("second retry did not reach global retry admission")
	}
	cancelSecond()
	select {
	case attempted := <-secondDone:
		require.Zero(t, attempted)
	case <-time.After(time.Second):
		cancelFirst()
		unlocksOwner()
		t.Fatal("second retry ignored cancellation while waiting for global retry admission")
	}
	require.Equal(t, 1, gossip.Len())

	cancelFirst()
	require.Equal(t, 1, <-firstDone)
	unlocksOwner()
}

func TestRetryPendingExecutionPayloadEnvelopesDropsFinalizedWork(t *testing.T) {
	gossip, err := lru.New[common.Hash, *pendingExecutionPayloadEnvelopeEntry](16)
	require.NoError(t, err)
	root := common.Hash{1}
	gossip.Add(root, &pendingExecutionPayloadEnvelopeEntry{
		createdAt: time.Now(),
		envelope:  &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}},
	})
	f := &ForkChoiceStore{
		beaconCfg:        &clparams.MainnetBeaconConfig,
		forkGraph:        payloadVoteForkGraph{block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1}}},
		pendingEnvelopes: gossip,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1})

	require.Zero(t, f.RetryPendingExecutionPayloadEnvelopes(t.Context(), 1))
	require.Zero(t, gossip.Len())
}

func TestEnvelopeOwnershipAndPruneDoNotGloballyExcludeEachOther(t *testing.T) {
	root := common.HexToHash("0x1234")
	t.Run("active envelope owner does not block prune", func(t *testing.T) {
		pruneStarted := make(chan struct{})
		releasePrune := make(chan struct{})
		f := &ForkChoiceStore{forkGraph: payloadVoteForkGraph{pruneStarted: pruneStarted, releasePrune: releasePrune}}
		unlockOwner := f.lockEnvelopeOwner(root)
		f.queuedPrunes = []uint64{1}
		pruneDone := make(chan struct{})
		go func() {
			f.drainQueuedWork()
			close(pruneDone)
		}()
		select {
		case <-pruneStarted:
		case <-time.After(time.Second):
			t.Fatal("active envelope owner blocked prune")
		}
		unlockOwner()
		close(releasePrune)
		<-pruneDone
	})

	t.Run("active prune does not block envelope owner", func(t *testing.T) {
		pruneStarted := make(chan struct{})
		releasePrune := make(chan struct{})
		f := &ForkChoiceStore{forkGraph: payloadVoteForkGraph{pruneStarted: pruneStarted, releasePrune: releasePrune}}
		f.queuedPrunes = []uint64{1}
		pruneDone := make(chan struct{})
		go func() {
			f.drainQueuedWork()
			close(pruneDone)
		}()
		<-pruneStarted
		ownerAcquired := make(chan func(), 1)
		go func() { ownerAcquired <- f.lockEnvelopeOwner(root) }()
		select {
		case unlockOwner := <-ownerAcquired:
			unlockOwner()
		case <-time.After(time.Second):
			t.Fatal("active prune blocked unrelated envelope owner")
		}
		close(releasePrune)
		<-pruneDone
	})
}

func TestValidatePayloadWithELDoesNotRelockForkChoiceMu(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, tt := range []struct {
		name      string
		status    execution_client.PayloadStatus
		engineErr error
		wantErr   bool
	}{
		{
			name:   "validated",
			status: execution_client.PayloadStatusValidated,
		},
		{
			name:    "invalidated",
			status:  execution_client.PayloadStatusInvalidated,
			wantErr: true,
		},
		{
			name:      "invalidated with validation error",
			status:    execution_client.PayloadStatusInvalidated,
			engineErr: errors.New("invalid payload"),
			wantErr:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engine.EXPECT().
				NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tt.status, tt.engineErr)

			verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](16)
			require.NoError(t, err)
			executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
			require.NoError(t, err)
			payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
			require.NoError(t, err)
			executionPayloadGasLimit, err := lru.New[common.Hash, uint64](16)
			require.NoError(t, err)

			blockRoot := common.HexToHash("0x1234")
			executionBlockHash := common.HexToHash("0xabcd")
			invalidatedHeader := common.Hash{}
			f := &ForkChoiceStore{
				beaconCfg:                cfg,
				engine:                   engine,
				forkGraph:                payloadVoteForkGraph{invalidatedHeader: &invalidatedHeader},
				verifiedExecutionPayload: verifiedExecutionPayload,
				executionPayloadStatus:   executionPayloadStatus,
				payloadStatusByRoot:      payloadStatusByRoot,
				executionPayloadGasLimit: executionPayloadGasLimit,
			}
			envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
			envelope.Payload.BlockHash = executionBlockHash
			body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
			body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
				Message: &cltypes.ExecutionPayloadBid{
					BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
				},
			}
			block := &cltypes.SignedBeaconBlock{
				Block: &cltypes.BeaconBlock{
					Body: body,
				},
			}

			done := make(chan error, 1)
			go func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				_, err := f.validatePayloadWithELLocked(context.Background(), envelope, block, blockRoot)
				done <- err
			}()

			select {
			case err := <-done:
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("validatePayloadWithELLocked blocked while forkchoice mutex was already held")
			}
			require.False(t, f.IsPayloadVerified(blockRoot))
			if tt.status == execution_client.PayloadStatusInvalidated {
				require.Equal(t, blockRoot, invalidatedHeader)
			}
		})
	}
}

func TestValidatePayloadWithELReleasesForkChoiceMuDuringNewPayload(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engineStarted := make(chan struct{})
	releaseEngine := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(engineStarted)
			<-releaseEngine
			return execution_client.PayloadStatusValidated, nil
		})

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](16)
	require.NoError(t, err)
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](16)
	require.NoError(t, err)

	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                payloadVoteForkGraph{},
		verifiedExecutionPayload: verifiedExecutionPayload,
		executionPayloadStatus:   executionPayloadStatus,
		payloadStatusByRoot:      payloadStatusByRoot,
		executionPayloadGasLimit: executionPayloadGasLimit,
	}
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Body: body}}
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
	envelope.Payload.BlockHash = common.HexToHash("0xabcd")

	validationDone := make(chan error, 1)
	go func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		_, err := f.validatePayloadWithELLocked(context.Background(), envelope, block, common.HexToHash("0x1234"))
		validationDone <- err
	}()
	<-engineStarted

	lockAcquired := make(chan struct{})
	go func() {
		f.mu.Lock()
		close(lockAcquired)
		f.mu.Unlock()
	}()
	acquiredBeforeRelease := false
	select {
	case <-lockAcquired:
		acquiredBeforeRelease = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseEngine)
	require.NoError(t, <-validationDone)
	require.True(t, acquiredBeforeRelease, "forkchoice mutex stayed locked during NewPayload")
}

func TestValidatePayloadWithELDoesNotCoalesceDifferentPayloads(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
			} else {
				close(secondStarted)
			}
			return execution_client.PayloadStatusValidated, nil
		})

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](16)
	require.NoError(t, err)
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](16)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                payloadVoteForkGraph{},
		verifiedExecutionPayload: verifiedExecutionPayload,
		executionPayloadStatus:   executionPayloadStatus,
		payloadStatusByRoot:      payloadStatusByRoot,
		executionPayloadGasLimit: executionPayloadGasLimit,
	}
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48)},
	}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Body: body}}
	blockRoot := common.HexToHash("0x1234")
	first := cltypes.NewExecutionPayloadEnvelope(cfg)
	first.BeaconBlockRoot = blockRoot
	first.Payload.BlockHash = common.HexToHash("0xabcd")
	second := cltypes.NewExecutionPayloadEnvelope(cfg)
	second.BeaconBlockRoot = blockRoot
	second.Payload.BlockHash = first.Payload.BlockHash
	second.Payload.GasUsed = 1

	results := make(chan error, 2)
	validate := func(envelope *cltypes.ExecutionPayloadEnvelope) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_, err := f.validatePayloadWithELLocked(context.Background(), envelope, block, blockRoot)
		results <- err
	}
	go validate(first)
	<-firstStarted
	go validate(second)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		close(releaseFirst)
		t.Fatal("different payload was coalesced with in-flight validation")
	}
	close(releaseFirst)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
}

func TestNewPayloadCoalescesSameKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	started := make(chan struct{})
	release := make(chan struct{})
	expectedErr := errors.New("payload rejected")
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(started)
			<-release
			return execution_client.PayloadStatusInvalidated, expectedErr
		})
	f := &ForkChoiceStore{engine: engine}
	key := hashWithFirstByte(1)
	type result struct {
		status execution_client.PayloadStatus
		err    error
	}
	results := make(chan result, 2)
	validate := func(acquired chan<- struct{}) {
		f.mu.Lock()
		if acquired != nil {
			close(acquired)
		}
		status, err := f.newPayloadLocked(context.Background(), key, nil, nil, nil, nil)
		f.mu.Unlock()
		results <- result{status: status, err: err}
	}

	go validate(nil)
	<-started
	followerAcquired := make(chan struct{})
	go validate(followerAcquired)
	<-followerAcquired
	f.mu.Lock()
	close(release)
	f.mu.Unlock()
	for range 2 {
		got := <-results
		require.EqualValues(t, execution_client.PayloadStatusInvalidated, got.status)
		require.ErrorIs(t, got.err, expectedErr)
	}
}

func TestNewPayloadCanceledWaiterDoesNotCancelLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	started := make(chan struct{})
	release := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(started)
			<-release
			return execution_client.PayloadStatusValidated, nil
		})
	f := &ForkChoiceStore{engine: engine}
	key := hashWithFirstByte(1)
	leaderDone := make(chan error, 1)
	go func() {
		f.mu.Lock()
		_, err := f.newPayloadLocked(context.Background(), key, nil, nil, nil, nil)
		f.mu.Unlock()
		leaderDone <- err
	}()
	<-started

	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	f.mu.Lock()
	_, err := f.newPayloadLocked(waiterCtx, key, nil, nil, nil, nil)
	f.mu.Unlock()
	require.ErrorIs(t, err, context.Canceled)

	close(release)
	require.NoError(t, <-leaderDone)
}

func TestNewPayloadConcurrencyIsBounded(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			current := active.Add(1)
			for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return execution_client.PayloadStatusValidated, nil
		})
	f := &ForkChoiceStore{engine: engine}
	done := make(chan struct{}, 3)
	for i := range 3 {
		go func(key byte) {
			f.mu.Lock()
			_, _ = f.newPayloadLocked(context.Background(), hashWithFirstByte(key), nil, nil, nil, nil)
			f.mu.Unlock()
			done <- struct{}{}
		}(byte(i + 1))
	}
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than two NewPayload calls ran concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-started
	for range 3 {
		<-done
	}
	require.Equal(t, int32(2), maximum.Load())
}

func TestNewPayloadPanicRestoresForkChoiceState(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	var calls atomic.Int32
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			if calls.Add(1) == 1 {
				panic("engine panic")
			}
			return execution_client.PayloadStatusValidated, nil
		})
	f := &ForkChoiceStore{engine: engine}
	key := hashWithFirstByte(1)
	func() {
		defer func() { require.Equal(t, "engine panic", recover()) }()
		f.mu.Lock()
		defer f.mu.Unlock()
		_, _ = f.newPayloadLocked(context.Background(), key, nil, nil, nil, nil)
	}()

	done := make(chan error, 2)
	for _, retryKey := range []common.Hash{key, hashWithFirstByte(2)} {
		go func() {
			f.mu.Lock()
			_, err := f.newPayloadLocked(context.Background(), retryKey, nil, nil, nil, nil)
			f.mu.Unlock()
			done <- err
		}()
	}
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("payload validation remained blocked after engine panic")
		}
	}
}

func hashWithFirstByte(value byte) common.Hash {
	var hash common.Hash
	hash[0] = value
	return hash
}
