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
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

type envelopeIndexTestForkGraph struct {
	fork_graph.ForkGraph
	persisted   *cltypes.SignedExecutionPayloadEnvelope
	reads       atomic.Int32
	readStarted chan<- struct{}
	releaseRead <-chan struct{}
}

func (g *envelopeIndexTestForkGraph) HasEnvelope(common.Hash) bool { return true }

func (g *envelopeIndexTestForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	g.reads.Add(1)
	if g.readStarted != nil {
		g.readStarted <- struct{}{}
	}
	if g.releaseRead != nil {
		<-g.releaseRead
	}
	return g.persisted, nil
}

type envelopeIndexTestDB struct {
	kv.RwDB
	updates       atomic.Int32
	failUpdates   int32
	alwaysFail    bool
	updateStarted chan<- struct{}
	releaseUpdate <-chan struct{}
	viewCalls     atomic.Int32
	viewCompleted chan<- struct{}
}

type envelopeIndexWaitContext struct {
	context.Context
	afterView atomic.Bool
	waiting   chan<- struct{}
}

func (ctx *envelopeIndexWaitContext) Done() <-chan struct{} {
	if ctx.afterView.Load() {
		select {
		case ctx.waiting <- struct{}{}:
		default:
		}
	}
	return ctx.Context.Done()
}

func (db *envelopeIndexTestDB) View(ctx context.Context, f func(kv.Tx) error) error {
	db.viewCalls.Add(1)
	err := db.RwDB.View(ctx, f)
	if waitCtx, ok := ctx.(*envelopeIndexWaitContext); ok {
		waitCtx.afterView.Store(true)
	}
	if db.viewCompleted != nil {
		db.viewCompleted <- struct{}{}
	}
	return err
}

func (db *envelopeIndexTestDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.updates.Add(1)
	if db.updateStarted != nil {
		select {
		case db.updateStarted <- struct{}{}:
		default:
		}
	}
	if db.releaseUpdate != nil {
		<-db.releaseUpdate
	}
	if db.alwaysFail || atomic.AddInt32(&db.failUpdates, -1) >= 0 {
		return errors.New("injected index update failure")
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

func TestOnExecutionPayloadRejectsNilWithdrawalBeforeForkchoice(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.Withdrawals.Append(nil)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	f := &ForkChoiceStore{beaconCfg: cfg}

	require.NotPanics(t, func() {
		err := f.OnExecutionPayload(context.Background(), &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}, false, true)
		require.ErrorContains(t, err, "nil withdrawal at index 0")
	})
}

func TestExecutionPayloadIngressRejectsUnpersistableEnvelopeBeforeForkchoice(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxTransactionsPerPayload = 1
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}})
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	signedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}

	for _, test := range []struct {
		name string
		call func(*ForkChoiceStore) error
	}{
		{name: "remote", call: func(f *ForkChoiceStore) error {
			return f.OnExecutionPayload(context.Background(), signedEnvelope, false, true)
		}},
		{name: "local", call: func(f *ForkChoiceStore) error {
			return f.ApplyLocalSelfBuildEnvelope(context.Background(), signedEnvelope)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := &ForkChoiceStore{beaconCfg: &cfg}
			require.NotPanics(t, func() {
				require.ErrorContains(t, test.call(f), "too many transactions")
			})
		})
	}
}

func TestExecutionPayloadDuplicateSkipsDiskAndIndexWriteWhenAlreadyIndexed(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	duplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)

	for _, test := range []struct {
		name string
		call func(*ForkChoiceStore) error
	}{
		{name: "remote", call: func(f *ForkChoiceStore) error {
			return f.OnExecutionPayload(context.Background(), duplicate, false, true)
		}},
		{name: "local", call: func(f *ForkChoiceStore) error {
			return f.ApplyLocalSelfBuildEnvelope(context.Background(), duplicate)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			baseDB := memdb.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, baseDB.Update(context.Background(), func(tx kv.RwTx) error {
				return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, root, persisted.Message)
			}))
			db := &envelopeIndexTestDB{RwDB: baseDB}
			graph := &envelopeIndexTestForkGraph{persisted: persisted}
			store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

			require.NoError(t, test.call(store))
			require.Zero(t, graph.reads.Load())
			require.Zero(t, db.updates.Load())
		})
	}
}

func TestExecutionPayloadDuplicateWithoutDBSkipsDisk(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	envelope := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	graph := &envelopeIndexTestForkGraph{persisted: envelope}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph}

	require.NoError(t, store.OnExecutionPayload(context.Background(), envelope, false, true))
	require.Zero(t, graph.reads.Load())
}

func TestExecutionPayloadDuplicateRepairsMissingIndicesFromPersistedEnvelope(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	untrustedDuplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)
	baseDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	db := &envelopeIndexTestDB{RwDB: baseDB, failUpdates: 1}
	graph := &envelopeIndexTestForkGraph{persisted: persisted}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

	err := store.OnExecutionPayload(context.Background(), untrustedDuplicate, false, true)
	require.ErrorContains(t, err, "injected index update failure")
	require.NoError(t, store.OnExecutionPayload(context.Background(), untrustedDuplicate, false, true))
	require.Equal(t, int32(2), graph.reads.Load())
	require.Equal(t, int32(2), db.updates.Load())
	require.NoError(t, baseDB.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
		require.NoError(t, err)
		require.NotNil(t, blockNumber)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestExecutionPayloadDuplicateRepairsPartialIndices(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	duplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)

	for _, test := range []struct {
		name string
		seed func(kv.RwTx) error
	}{
		{name: "number only", seed: func(tx kv.RwTx) error {
			return beacon_indicies.WriteExecutionBlockNumber(tx, root, 99)
		}},
		{name: "hash only", seed: func(tx kv.RwTx) error {
			return beacon_indicies.WriteExecutionBlockHash(tx, root, common.HexToHash("0xcccc"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			baseDB := memdb.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, baseDB.Update(context.Background(), test.seed))
			db := &envelopeIndexTestDB{RwDB: baseDB}
			graph := &envelopeIndexTestForkGraph{persisted: persisted}
			store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

			require.NoError(t, store.OnExecutionPayload(context.Background(), duplicate, false, true))
			require.Equal(t, int32(1), graph.reads.Load())
			require.Equal(t, int32(1), db.updates.Load())
			requireEnvelopeIndices(t, baseDB, root, persisted.Message.Payload.BlockNumber, persisted.Message.Payload.BlockHash)
		})
	}
}

func TestExecutionPayloadAppliedIndexFailureRepairsFromPersistedEnvelope(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	untrustedDuplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)
	baseDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	db := &envelopeIndexTestDB{RwDB: baseDB, failUpdates: 1}
	graph := &envelopeIndexTestForkGraph{persisted: persisted}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

	err := store.writeExecutionPayloadEnvelopeIndices(context.Background(), root, persisted.Message, true)
	require.ErrorContains(t, err, "injected index update failure")
	require.Zero(t, db.viewCalls.Load())
	require.Zero(t, graph.reads.Load())
	require.NoError(t, store.OnExecutionPayload(context.Background(), untrustedDuplicate, false, true))
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, int32(2), db.updates.Load())
	requireEnvelopeIndices(t, baseDB, root, persisted.Message.Payload.BlockNumber, persisted.Message.Payload.BlockHash)
}

func TestExecutionPayloadConcurrentDuplicatesCoalesceMissingIndexRepair(t *testing.T) {
	const callers = 8
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	duplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)
	readStarted := make(chan struct{}, callers)
	releaseRead := make(chan struct{})
	graph := &envelopeIndexTestForkGraph{persisted: persisted, readStarted: readStarted, releaseRead: releaseRead}
	db := &envelopeIndexTestDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB)}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

	errs := make(chan error, callers)
	go func() { errs <- store.OnExecutionPayload(context.Background(), duplicate, false, true) }()
	<-readStarted
	for range callers - 1 {
		waiting := make(chan struct{}, 1)
		waitCtx := &envelopeIndexWaitContext{Context: context.Background(), waiting: waiting}
		go func() {
			errs <- store.OnExecutionPayload(waitCtx, duplicate, false, true)
		}()
		<-waiting
	}
	require.Equal(t, int32(1), graph.reads.Load())
	close(releaseRead)
	for range callers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, int32(1), db.updates.Load())
}

func TestExecutionPayloadConcurrentDuplicatesShareRepairFailureThenRetry(t *testing.T) {
	const callers = 8
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	duplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)
	updateStarted := make(chan struct{}, 1)
	releaseUpdate := make(chan struct{})
	baseDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	db := &envelopeIndexTestDB{
		RwDB:          baseDB,
		alwaysFail:    true,
		updateStarted: updateStarted,
		releaseUpdate: releaseUpdate,
	}
	graph := &envelopeIndexTestForkGraph{persisted: persisted}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

	errs := make(chan error, callers)
	go func() { errs <- store.OnExecutionPayload(context.Background(), duplicate, false, true) }()
	<-updateStarted
	for range callers - 1 {
		waiting := make(chan struct{}, 1)
		waitCtx := &envelopeIndexWaitContext{Context: context.Background(), waiting: waiting}
		go func() {
			errs <- store.OnExecutionPayload(waitCtx, duplicate, false, true)
		}()
		<-waiting
	}
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, int32(1), db.updates.Load())
	close(releaseUpdate)
	for range callers {
		require.ErrorContains(t, <-errs, "injected index update failure")
	}
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, int32(1), db.updates.Load())

	db.alwaysFail = false
	require.NoError(t, store.OnExecutionPayload(context.Background(), duplicate, false, true))
	require.Equal(t, int32(2), graph.reads.Load())
	require.Equal(t, int32(2), db.updates.Load())
	requireEnvelopeIndices(t, baseDB, root, persisted.Message.Payload.BlockNumber, persisted.Message.Payload.BlockHash)
}

func TestExecutionPayloadCanceledRepairWaiterReturnsBeforeOwnerFinishes(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	duplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)
	readStarted := make(chan struct{}, 1)
	releaseRead := make(chan struct{})
	viewCompleted := make(chan struct{}, 6)
	graph := &envelopeIndexTestForkGraph{persisted: persisted, readStarted: readStarted, releaseRead: releaseRead}
	db := &envelopeIndexTestDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB), viewCompleted: viewCompleted}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- store.OnExecutionPayload(context.Background(), duplicate, false, true) }()
	<-readStarted
	for range 2 {
		<-viewCompleted
	}

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- store.OnExecutionPayload(waiterCtx, duplicate, false, true) }()
	<-viewCompleted
	cancelWaiter()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(releaseRead)
		require.NoError(t, <-ownerDone)
		t.Fatal("canceled waiter did not return while repair owner remained blocked")
	}
	require.Equal(t, int32(1), graph.reads.Load())
	require.Zero(t, db.updates.Load())
	close(releaseRead)
	require.NoError(t, <-ownerDone)
	require.Equal(t, int32(1), db.updates.Load())
}

func TestExecutionPayloadCanceledInitiatorReturnsWhileSharedRepairContinues(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	persisted := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xaaaa"), 11)
	duplicate := executionPayloadEnvelopeForIndexTest(cfg, root, common.HexToHash("0xbbbb"), 22)
	readStarted := make(chan struct{}, 1)
	releaseRead := make(chan struct{})
	viewCompleted := make(chan struct{}, 6)
	graph := &envelopeIndexTestForkGraph{persisted: persisted, readStarted: readStarted, releaseRead: releaseRead}
	db := &envelopeIndexTestDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB), viewCompleted: viewCompleted}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db}

	initiatorCtx, cancelInitiator := context.WithCancel(context.Background())
	initiatorDone := make(chan error, 1)
	go func() { initiatorDone <- store.OnExecutionPayload(initiatorCtx, duplicate, false, true) }()
	<-readStarted
	for range 2 {
		<-viewCompleted
	}

	waiterDone := make(chan error, 1)
	go func() { waiterDone <- store.OnExecutionPayload(context.Background(), duplicate, false, true) }()
	<-viewCompleted
	cancelInitiator()
	select {
	case err := <-initiatorDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(releaseRead)
		<-initiatorDone
		require.NoError(t, <-waiterDone)
		t.Fatal("canceled initiating caller did not return while shared repair remained blocked")
	}
	close(releaseRead)
	require.NoError(t, <-waiterDone)
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, int32(1), db.updates.Load())
}

func executionPayloadEnvelopeForIndexTest(cfg *clparams.BeaconChainConfig, root, blockHash common.Hash, blockNumber uint64) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
	envelope.BeaconBlockRoot = root
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	envelope.Payload.BlockHash = blockHash
	envelope.Payload.BlockNumber = blockNumber
	return &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}
}

func requireEnvelopeIndices(t *testing.T, db kv.RoDB, root common.Hash, blockNumber uint64, blockHash common.Hash) {
	t.Helper()
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		storedNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
		require.NoError(t, err)
		require.NotNil(t, storedNumber)
		require.Equal(t, blockNumber, *storedNumber)
		storedHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, blockHash, storedHash)
		return nil
	}))
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

// TestValidatePayloadWithEL_NoEngine tests that validatePayloadWithEL returns nil when there's no engine
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

	err := f.validatePayloadWithEL(context.TODO(), envelope, block, common.Hash{})
	require.NoError(t, err)
}

func TestValidatePayloadWithELDoesNotRelockForkChoiceMu(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, tt := range []struct {
		name       string
		status     execution_client.PayloadStatus
		wantErr    bool
		wantVerify bool
	}{
		{
			name:       "validated",
			status:     execution_client.PayloadStatusValidated,
			wantVerify: true,
		},
		{
			name:    "invalidated",
			status:  execution_client.PayloadStatusInvalidated,
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engine.EXPECT().
				NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tt.status, nil)

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
			envelope := &cltypes.ExecutionPayloadEnvelope{
				Payload: &cltypes.Eth1Block{BlockHash: executionBlockHash},
			}
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
				done <- f.validatePayloadWithEL(context.Background(), envelope, block, blockRoot)
			}()

			select {
			case err := <-done:
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("validatePayloadWithEL blocked while forkchoice mutex was already held")
			}
			require.Equal(t, tt.wantVerify, f.IsPayloadVerified(blockRoot))
			if tt.status == execution_client.PayloadStatusInvalidated {
				require.Equal(t, blockRoot, invalidatedHeader)
			}
		})
	}
}
