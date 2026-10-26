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

package handler

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	syncmock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/phase1/stages"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type preparationExecutionClient struct {
	*execution_client.ExecutionClientDirect
	forkChoiceCalls       int
	productionID          []byte
	collectedID           []byte
	startPayloadBuildHook func()
}

func (c *preparationExecutionClient) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attrs *engine_types.PayloadAttributes, version clparams.StateVersion) ([]byte, error) {
	c.forkChoiceCalls++
	id, err := c.ExecutionClientDirect.ForkChoiceUpdate(ctx, finalized, safe, head, attrs, version)
	if attrs != nil {
		c.productionID = bytes.Clone(id)
	}
	return id, err
}

func (c *preparationExecutionClient) GetAssembledBlock(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	c.collectedID = bytes.Clone(id)
	return c.ExecutionClientDirect.GetAssembledBlock(ctx, id, version)
}

func (c *preparationExecutionClient) StartPayloadBuild(ctx context.Context, head common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
	if c.startPayloadBuildHook != nil {
		c.startPayloadBuildHook()
	}
	return c.ExecutionClientDirect.StartPayloadBuild(ctx, head, attrs)
}

func TestGloasFullParentPreparationWithExecutionModule(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	ctx := t.Context()
	fixture := newGloasPreparationIntegrationFixture(t)
	require.True(t, fixture.store.IsPayloadVerified(fixture.parentRoot))
	executionHead, _, _, err := fixture.reader.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, fixture.seedHash, executionHead, "NewPayload must leave fork choice at the EMPTY parent")
	head, executionHead := fixture.runForkChoice(t)
	require.Equal(t, forkchoice.ForkChoiceNode{Root: fixture.parentRoot, PayloadStatus: cltypes.PayloadStatusFull}, head)
	require.Equal(t, fixture.parentPayload.Payload.BlockHash, executionHead)
	require.NoError(t, fixture.handler.indiciesDB.View(ctx, func(tx kv.Tx) error {
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, fixture.parentSlot)
		require.NoError(t, err)
		require.Equal(t, fixture.parentRoot, root, "the ForkChoice stage must publish the canonical head")
		return nil
	}))

	targetSlot := fixture.parentSlot + 1
	proposer, err := fixture.parentState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	fixture.handler.validatorParams.SetFeeRecipient(proposer, common.Address{0x42})
	callsBeforePreparation := fixture.engine.forkChoiceCalls
	preparationCtx, cancel := context.WithCancel(ctx)
	done := fixture.handler.StartPayloadPreparation(preparationCtx)
	t.Cleanup(func() { cancel(); <-done })
	var prepared preparedPayloadRecord
	require.Eventually(t, func() bool {
		fixture.handler.preparedPayload.mu.Lock()
		defer fixture.handler.preparedPayload.mu.Unlock()
		prepared = fixture.handler.preparedPayload.payloads[targetSlot]
		return len(prepared.id) != 0
	}, time.Until(fixture.handler.ethClock.GetSlotTime(targetSlot)), 10*time.Millisecond)
	cancel()
	<-done
	require.Equal(t, callsBeforePreparation, fixture.engine.forkChoiceCalls)
	require.NotEmpty(t, prepared.id)
	require.True(t, prepared.primedAt.Before(fixture.handler.ethClock.GetSlotTime(targetSlot)))
	require.NoError(t, common.Sleep(ctx, time.Until(fixture.handler.ethClock.GetSlotTime(targetSlot))))
	warmup := time.Since(prepared.primedAt)
	require.Greater(t, warmup, minimumPreparationLead)
	t.Logf("FULL-parent builder warmup before production: %s", warmup)

	productionState, err := fixture.parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(productionState, targetSlot))
	body, _, err := fixture.handler.produceBeaconBody(ctx, fixture.parentSlot, fixture.parentRoot, productionState, targetSlot, common.Bytes96{0xc0}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, fixture.parentPayload.Payload.BlockHash, body.SignedExecutionPayloadBid.Message.ParentBlockHash)
	require.Equal(t, prepared.id, fixture.engine.productionID)
	require.Equal(t, prepared.id, fixture.engine.collectedID)
	_, _, mismatch := fixture.handler.preparedPayload.warmupAndMismatch(targetSlot, fixture.engine.productionID, time.Now())
	require.False(t, mismatch)
}

type gloasPreparationIntegrationFixture struct {
	module         *execmoduletester.ExecModuleTester
	reader         chainreader.ChainReaderWriterEth1
	engine         *preparationExecutionClient
	handler        *ApiHandler
	store          *forkchoice.ForkChoiceStore
	synced         *synced_data.SyncedDataManager
	anchor         *state.CachingBeaconState
	seedHash       common.Hash
	parentState    *state.CachingBeaconState
	parentBlock    *cltypes.SignedBeaconBlock
	parentRoot     common.Hash
	parentSlot     uint64
	parentPayload  *selfBuildPayload
	parentEnvelope *cltypes.SignedExecutionPayloadEnvelope
}

func newGloasPreparationIntegrationFixture(t *testing.T) *gloasPreparationIntegrationFixture {
	t.Helper()
	module := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	seed, err := module.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, module.InsertChain(seed))
	reader := chainreader.NewChainReaderEth1(module.ChainConfig, module.ExecModule, time.Second)
	direct, err := execution_client.NewExecutionClientDirect(reader, nil)
	require.NoError(t, err)
	engine := &preparationExecutionClient{ExecutionClientDirect: direct}

	fixtureState, handler, _, _, _ := setupGloasPreparationTest(t)
	anchor := state.New(handler.beaconChainCfg)
	require.NoError(t, fixtureState.CopyInto(anchor))
	anchorSlot := (anchor.Slot()/handler.beaconChainCfg.SlotsPerEpoch + 1) * handler.beaconChainCfg.SlotsPerEpoch
	require.NoError(t, transition.DefaultMachine.ProcessSlots(anchor, anchorSlot))
	anchor.SetGenesisTime(uint64(time.Now().Unix()) - (anchorSlot+1)*handler.beaconChainCfg.SecondsPerSlot - 2)
	seedHash := seed.TopBlock.Hash()
	anchor.SetLatestBlockHash(seedHash)
	anchor.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{ParentBlockHash: seedHash, GasLimit: seed.TopBlock.GasLimit()})
	anchorTemplate, err := anchor.Copy()
	require.NoError(t, err)
	clock := eth_clock.NewEthereumClock(anchor.GenesisTime(), anchor.GenesisValidatorsRoot(), handler.beaconChainCfg)
	synced := synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	graph, err := fork_graph.NewForkGraphDisk(anchor, synced, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	handler.emitters = beaconevents.NewEventEmitter()
	store, err := forkchoice.NewForkChoiceStore(clock, anchor, engine, pool.NewOperationsPool(handler.beaconChainCfg), graph, handler.emitters, synced, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(), handler.validatorParams, false, nil)
	require.NoError(t, err)
	handler.engine, handler.forkchoiceStore, handler.syncedData, handler.ethClock = engine, store, synced, clock
	require.NoError(t, synced.OnHeadStateWithBlockRoot(anchor, store.AnchorRoot()))

	parentState, parentBlock, parentEnvelope, parentPayload, parentRoot, parentSlot := produceGloasIntegrationChild(t, handler, anchor, store.AnchorRoot(), anchor.LatestBlockHeader().Slot)
	store.OnTick(utils.SecondsToMilliseconds(anchor.GenesisTime() + parentSlot*handler.beaconChainCfg.SecondsPerSlot))
	applyGossipIntegrationEnvelope(t, handler.indiciesDB, store, parentBlock, parentEnvelope)

	return &gloasPreparationIntegrationFixture{
		module: module, reader: reader, engine: engine, handler: handler, store: store, synced: synced,
		anchor: anchorTemplate, seedHash: seedHash, parentState: parentState, parentBlock: parentBlock,
		parentRoot: parentRoot, parentSlot: parentSlot, parentPayload: parentPayload, parentEnvelope: parentEnvelope,
	}
}

func (f *gloasPreparationIntegrationFixture) runForkChoice(t *testing.T) (forkchoice.ForkChoiceNode, common.Hash) {
	t.Helper()
	head, executionHead := runGloasIntegrationForkChoice(t, f.store, f.engine, f.handler, f.synced)
	f.module.ExecModule.WaitIdle(t.Context())
	require.Equal(t, f.parentRoot, f.synced.HeadRoot(), "the ForkChoice stage must materialize its head")
	actualHead, _, _, err := f.reader.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.Equal(t, executionHead, actualHead)
	return head, executionHead
}

type preparationSentinel struct {
	sentinelproto.SentinelClient
}

func (*preparationSentinel) SetStatus(context.Context, *sentinelproto.Status, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return &sentinelproto.EmptyMessage{}, nil
}

func (*preparationSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func runGloasIntegrationForkChoice(
	t *testing.T,
	store *forkchoice.ForkChoiceStore,
	engine execution_client.ExecutionEngine,
	handler *ApiHandler,
	synced *synced_data.SyncedDataManager,
) (forkchoice.ForkChoiceNode, common.Hash) {
	t.Helper()
	anchor, err := store.GetStateAtBlockRoot(store.AnchorRoot())
	require.NoError(t, err)
	beaconRPC := rpc.NewBeaconRpcP2P(t.Context(), &preparationSentinel{}, handler.beaconChainCfg, handler.ethClock)
	cfg := stages.ClStagesCfg(
		t.Context(), beaconRPC, nil, handler.ethClock, handler.beaconChainCfg, anchor,
		engine, store, handler.indiciesDB, nil, handler.blockReader, datadir.New(t.TempDir()),
		clparams.CaplinConfig{}, synced, handler.emitters, handler.blobStoage,
		attestation_producer.New(handler.beaconChainCfg), nil,
	)
	// ForkChoice does not use the history collector. Its large mmap can overlap the race
	// detector's heap region, so release it before the stage starts allocating state data.
	require.NoError(t, cfg.Close())
	require.NoError(t, stages.ConsensusClStages().Stages[stages.ForkChoice].ActionFunc(t.Context(), log.Root(), cfg, stages.Args{}))

	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	executionHead, ready := store.GetExecutionHead(head)
	require.True(t, ready)
	return head, executionHead
}

func produceGloasIntegrationChild(
	t *testing.T,
	handler *ApiHandler,
	baseState *state.CachingBeaconState,
	baseRoot common.Hash,
	baseSlot uint64,
) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope, *selfBuildPayload, common.Hash, uint64) {
	t.Helper()
	targetSlot := baseState.Slot() + 1
	proposalState, err := baseState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(proposalState, targetSlot))
	block, err := handler.produceBlock(t.Context(), 0, baseSlot, baseRoot, proposalState, targetSlot, common.Bytes96{0xc0}, common.Hash{})
	require.NoError(t, err)
	postState, _, err := handler.processProducedBlock(proposalState, block)
	require.NoError(t, err)
	block.StateRoot, err = postState.HashSSZ()
	require.NoError(t, err)
	signedBlock := &cltypes.SignedBeaconBlock{Block: block.ToExecution().Block}
	root, err := signedBlock.Block.HashSSZ()
	require.NoError(t, err)
	payload, ok := handler.selfBuildPayloads.Get(block.BeaconBody.SignedExecutionPayloadBid.Message.BlockHash)
	require.True(t, ok)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload: payload.Payload, ExecutionRequests: payload.ExecutionRequests,
		BuilderIndex: clparams.BuilderIndexSelfBuild, BeaconBlockRoot: root, ParentBeaconBlockRoot: signedBlock.Block.ParentRoot,
	}}
	return postState, signedBlock, envelope, payload, root, targetSlot
}

func applyGossipIntegrationEnvelope(
	t *testing.T,
	db kv.RwDB,
	store *forkchoice.ForkChoiceStore,
	block *cltypes.SignedBeaconBlock,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
) {
	t.Helper()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, false)
	}))
	require.NoError(t, store.OnBlock(t.Context(), block, true, false, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	blockState, err := store.GetStateAtBlockRoot(root)
	require.NoError(t, err)
	require.NotNil(t, blockState)
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	// The fixture uses validator index + 1 as the BLS secret key. Reuse it so envelope
	// signatures can be checked without changing the stored validator keys.
	keyBytes := new(big.Int).SetUint64(block.Block.ProposerIndex + 1).FillBytes(make([]byte, 32))
	key, err := bls.NewPrivateKeyFromBytes(keyBytes)
	require.NoError(t, err)
	validator, err := blockState.ValidatorForValidatorIndex(int(block.Block.ProposerIndex))
	require.NoError(t, err)
	require.Equal(t, validator.PublicKey(), [48]byte(bls.CompressPublicKey(key.PublicKey())), "the signing key must match the fixture's proposer")
	signExecutionPayloadEnvelope(t, blockState, block.Block.Slot, envelope, key)
	service := services.NewExecutionPayloadService(t.Context(), store, blockState.BeaconConfig(), emitter, nil)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded, err := service.DecodeGossipMessage("", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.NoError(t, service.ProcessMessage(t.Context(), nil, decoded))
	select {
	case event := <-events:
		require.Equal(t, beaconevents.OpExecutionPayloadGossip, event.Event)
		require.Equal(t, common.Hash(root), event.Data.(*beaconevents.ExecutionPayloadGossipData).BlockRoot)
	default:
		t.Fatal("the gossip service did not accept and announce the envelope")
	}
}

func TestGloasCanonicalForkChoiceAdvancesExecutionHeadOverUnverifiedFullHead(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	fixture := newGloasPreparationIntegrationFixture(t)
	fixture.runForkChoice(t)
	_, childBlock, childEnvelope, childPayload, childRoot, childSlot := produceGloasIntegrationChild(
		t, fixture.handler, fixture.parentState, fixture.parentRoot, fixture.parentSlot,
	)

	importModule := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	importSeed, err := importModule.GenerateChain(1, nil)
	require.NoError(t, err)
	require.Equal(t, fixture.seedHash, importSeed.TopBlock.Hash())
	require.NoError(t, importModule.InsertChain(importSeed))
	importReader := chainreader.NewChainReaderEth1(importModule.ChainConfig, importModule.ExecModule, time.Second)
	importDirect, err := execution_client.NewExecutionClientDirect(importReader, nil)
	require.NoError(t, err)
	importEngine := &preparationExecutionClient{ExecutionClientDirect: importDirect}

	anchor, err := fixture.anchor.Copy()
	require.NoError(t, err)
	synced := synced_data.NewSyncedDataManager(fixture.handler.beaconChainCfg, true)
	graph, err := fork_graph.NewForkGraphDisk(anchor, synced, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	store, err := forkchoice.NewForkChoiceStore(
		fixture.handler.ethClock, anchor, importEngine, pool.NewOperationsPool(fixture.handler.beaconChainCfg), graph,
		beaconevents.NewEventEmitter(), synced, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(),
		fixture.handler.validatorParams, false, nil,
	)
	require.NoError(t, err)
	store.OnTick(utils.SecondsToMilliseconds(anchor.GenesisTime() + fixture.parentSlot*fixture.handler.beaconChainCfg.SecondsPerSlot))
	applyGossipIntegrationEnvelope(t, fixture.handler.indiciesDB, store, fixture.parentBlock, fixture.parentEnvelope)
	require.True(t, store.IsPayloadVerified(fixture.parentRoot))
	store.OnTick(utils.SecondsToMilliseconds(anchor.GenesisTime() + childSlot*fixture.handler.beaconChainCfg.SecondsPerSlot))
	applyGossipIntegrationEnvelope(t, fixture.handler.indiciesDB, store, childBlock, childEnvelope)
	require.False(t, store.IsPayloadVerified(childRoot))

	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, forkchoice.ForkChoiceNode{Root: childRoot, PayloadStatus: cltypes.PayloadStatusFull}, head)
	before, _, _, err := importReader.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, childPayload.Payload.BlockHash, before)

	_, sent := runGloasIntegrationForkChoice(t, store, importEngine, fixture.handler, synced)
	require.Equal(t, childPayload.Payload.BlockHash, sent)
	require.Equal(t, childRoot, synced.HeadRoot())
	importModule.ExecModule.WaitIdle(t.Context())
	after, _, _, err := importReader.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.Equal(t, childPayload.Payload.BlockHash, after)
}

func TestGloasPreparationDiscardsBuildAfterPayloadInvalidation(t *testing.T) {
	fixture := newGloasPreparationIntegrationFixture(t)
	fixture.runForkChoice(t)
	targetSlot := fixture.parentSlot + 1
	proposer, err := fixture.parentState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	fixture.handler.validatorParams.SetFeeRecipient(proposer, common.Address{0x42})
	fixture.engine.startPayloadBuildHook = func() {
		fixture.store.MarkPayloadStatus(fixture.parentRoot, fixture.parentPayload.Payload.BlockHash, execution_client.PayloadStatusInvalidated)
	}

	_, err = preparePayloadForTest(t, fixture.handler, targetSlot)

	require.ErrorIs(t, err, errGloasPayloadPending)
	require.Empty(t, fixture.handler.preparedPayload.payloads)
}

func TestGloasPreparationDiscardsBuildAfterHeadChange(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	fixture := newGloasPreparationIntegrationFixture(t)
	fixture.runForkChoice(t)
	_, childBlock, _, _, childRoot, childSlot := produceGloasIntegrationChild(
		t, fixture.handler, fixture.parentState, fixture.parentRoot, fixture.parentSlot,
	)
	targetSlot := childSlot + 1
	proposer, err := fixture.parentState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	fixture.handler.validatorParams.SetFeeRecipient(proposer, common.Address{0x42})
	fixture.engine.startPayloadBuildHook = func() {
		fixture.store.OnTick(utils.SecondsToMilliseconds(fixture.anchor.GenesisTime() + childSlot*fixture.handler.beaconChainCfg.SecondsPerSlot))
		require.NoError(t, fixture.store.OnBlock(t.Context(), childBlock, true, false, false))
	}

	_, err = preparePayloadForTest(t, fixture.handler, targetSlot)

	// Import cancellation takes priority over the final head check.
	require.ErrorIs(t, err, forkchoice.ErrPayloadImportInProgress)
	head, _, err := fixture.store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, childRoot, head.Root)
	require.Empty(t, fixture.handler.preparedPayload.payloads)
}

func signExecutionPayloadEnvelope(
	t *testing.T,
	blockState *state.CachingBeaconState,
	blockSlot uint64,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	key *bls.PrivateKey,
) {
	t.Helper()
	domain, err := blockState.GetDomain(blockState.BeaconConfig().DomainBeaconBuilder, state.GetEpochAtSlot(blockState.BeaconConfig(), blockSlot))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(envelope.Message, domain)
	require.NoError(t, err)
	copy(envelope.Signature[:], key.Sign(signingRoot[:]).Bytes())
}

type preparationCollectionClient struct {
	*execution_client.ExecutionClientDirect
	prepareErr      error
	productionID    []byte
	collectedID     []byte
	firstCollection time.Time
}

func (c *preparationCollectionClient) StartPayloadBuild(ctx context.Context, head common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	return c.ExecutionClientDirect.StartPayloadBuild(ctx, head, attrs)
}

func (c *preparationCollectionClient) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attrs *engine_types.PayloadAttributes, version clparams.StateVersion) ([]byte, error) {
	id, err := c.ExecutionClientDirect.ForkChoiceUpdate(ctx, finalized, safe, head, attrs, version)
	c.productionID = bytes.Clone(id)
	return id, err
}

func (c *preparationCollectionClient) GetAssembledBlock(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	if c.firstCollection.IsZero() {
		c.firstCollection = time.Now()
		c.collectedID = bytes.Clone(id)
	}
	return c.ExecutionClientDirect.GetAssembledBlock(ctx, id, version)
}

type preparationCollectionPath struct {
	name              string
	advanceToEpochEnd bool
	gloasAtCurrent    bool
	gloasAtNextEpoch  bool
	fullHead          bool
	buildOnFull       bool
	canonicalFull     bool
	wantParentFull    bool
}

type preparationCollectionFixture struct {
	handler         *ApiHandler
	engine          *preparationCollectionClient
	cfg             clparams.BeaconChainConfig
	headState       *state.CachingBeaconState
	productionState *state.CachingBeaconState
	parentHash      common.Hash
	fullHash        common.Hash
	baseRoot        common.Hash
	currentSlot     uint64
	targetSlot      uint64
	version         clparams.StateVersion
	proposer        uint64
	slotStart       time.Time
}

func newPreparationCollectionFixture(t *testing.T, path preparationCollectionPath) *preparationCollectionFixture {
	t.Helper()
	_, _, _, _, initialState, handler, _, synced, store, validators := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	cfg := *handler.beaconChainCfg
	cfg.SecondsPerSlot = 4
	currentEpoch := initialState.Slot() / cfg.SlotsPerEpoch
	if path.gloasAtNextEpoch {
		cfg.FuluForkEpoch = currentEpoch
		cfg.GloasForkEpoch = currentEpoch + 1
	} else if path.gloasAtCurrent {
		cfg.FuluForkEpoch = currentEpoch
		cfg.GloasForkEpoch = currentEpoch
	}
	cfg.InitializeForkSchedule()
	handler.beaconChainCfg = &cfg
	headState := state.New(&cfg)
	require.NoError(t, initialState.CopyInto(headState))
	if cfg.FuluForkEpoch == currentEpoch {
		require.NoError(t, headState.UpgradeToFulu())
	}
	if cfg.GloasForkEpoch == currentEpoch {
		require.NoError(t, headState.UpgradeToGloas())
	}
	if path.advanceToEpochEnd {
		require.NoError(t, transition.DefaultMachine.ProcessSlots(headState, (currentEpoch+1)*cfg.SlotsPerEpoch-1))
	}
	currentSlot := headState.Slot()
	targetSlot := currentSlot + 1
	version := cfg.GetCurrentStateVersion(targetSlot / cfg.SlotsPerEpoch)
	timestamp := uint64(time.Now().Add(4 * time.Second).Unix())
	headState.SetGenesisTime(timestamp - targetSlot*cfg.SecondsPerSlot)
	latestHeader := headState.LatestBlockHeader()
	latestHeader.Slot = currentSlot
	headState.SetLatestBlockHeader(&latestHeader)

	elCfg := *chain.AllProtocolChanges
	if version.Before(clparams.FuluVersion) {
		elCfg.OsakaTime = nil
	}
	if version.Before(clparams.GloasVersion) {
		elCfg.AmsterdamTime = nil
	} else if path.gloasAtNextEpoch {
		elCfg.AmsterdamTime = &timestamp
	}
	module := execmoduletester.New(t, execmoduletester.WithChainConfig(&elCfg), execmoduletester.WithTxPool())
	blocks, err := module.GenerateChain(2, nil)
	require.NoError(t, err)
	require.NoError(t, module.InsertChain(blocks))
	parent, full := blocks.Blocks[0], blocks.TopBlock
	client, err := execution_client.NewExecutionClientDirect(chainreader.NewChainReaderEth1(&elCfg, module.ExecModule, time.Minute), nil)
	require.NoError(t, err)
	canonical := parent.Hash()
	if path.canonicalFull {
		canonical = full.Hash()
	}
	_, err = client.ForkChoiceUpdate(t.Context(), parent.Hash(), parent.Hash(), canonical, nil, version)
	require.NoError(t, err)
	module.ExecModule.WaitIdle(t.Context())
	engine := &preparationCollectionClient{ExecutionClientDirect: client}
	handler.engine = engine
	baseRoot := common.Hash{0x41}
	store.HeadVal = baseRoot
	store.HeadSlotVal = currentSlot
	store.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
	payloadHeader := headState.LatestExecutionPayloadHeader()
	payloadHeader.BlockHash = parent.Hash()
	payloadHeader.BlockNumber = parent.NumberU64()
	payloadHeader.GasLimit = parent.GasLimit()
	headState.SetLatestExecutionPayloadHeader(payloadHeader)
	if headState.Version() >= clparams.GloasVersion {
		headState.SetLatestBlockHash(parent.Hash())
		headState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
			ParentBlockHash: parent.Hash(), BlockHash: full.Hash(), Slot: currentSlot,
			GasLimit: parent.GasLimit(), BuilderIndex: clparams.BuilderIndexSelfBuild,
		})
	}
	if path.fullHead {
		store.HeadPayloadStatusVal = cltypes.PayloadStatusFull
		store.VerifiedPayloads = map[common.Hash]bool{baseRoot: true}
		buildOnFull := path.buildOnFull
		store.ShouldBuildOnFullVal = &buildOnFull
		store.SetEnvelope(baseRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion),
		}})
	}
	productionState, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(productionState, targetSlot))
	proposer, err := productionState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validators.SetFeeRecipient(proposer, common.Address{0x42})
	syncedMock := synced.(*syncmock.MockSyncedData)
	syncedMock.EXPECT().SelectedHead().Return(baseRoot, currentSlot, true).AnyTimes()
	syncedMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
		return view(headState, baseRoot, currentSlot)
	}).AnyTimes()
	fixture := &preparationCollectionFixture{
		handler: handler, engine: engine, cfg: cfg, headState: headState, productionState: productionState,
		parentHash: parent.Hash(), fullHash: full.Hash(), baseRoot: baseRoot, currentSlot: currentSlot,
		targetSlot: targetSlot, version: version, proposer: proposer,
	}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetSlotTime(targetSlot).DoAndReturn(func(uint64) time.Time {
		// Only collection timing depends on elapsed wall-clock time in this test.
		if fixture.slotStart.IsZero() {
			return time.Now().Add(time.Duration(cfg.SecondsPerSlot) * time.Second)
		}
		return fixture.slotStart
	}).AnyTimes()
	handler.ethClock = clock
	return fixture
}

func TestPayloadPreparationCollectionWithExecutionModule(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	paths := []preparationCollectionPath{
		{name: "ordinary"},
		{name: "epoch", advanceToEpochEnd: true},
		{name: "first_gloas", advanceToEpochEnd: true, gloasAtNextEpoch: true},
		{name: "empty", gloasAtCurrent: true},
		{name: "full", gloasAtCurrent: true, fullHead: true, buildOnFull: true, canonicalFull: true, wantParentFull: true},
		{name: "full_to_empty", gloasAtCurrent: true, fullHead: true, canonicalFull: true},
	}
	for _, path := range paths {
		needsForkChoice := path.fullHead && !path.buildOnFull
		preparations := []string{"prepared", "absent", "mismatch"}
		if !needsForkChoice {
			preparations = append(preparations, "failed")
		}
		for _, preparation := range preparations {
			t.Run(path.name+"/"+preparation, func(t *testing.T) {
				fixture := newPreparationCollectionFixture(t, path)
				if preparation == "failed" {
					fixture.engine.prepareErr = errors.New("temporary execution failure")
				}
				if preparation != "absent" {
					_, err := preparePayloadForTest(t, fixture.handler, fixture.targetSlot)
					switch {
					case needsForkChoice:
						require.ErrorIs(t, err, errGloasPathNeedsForkChoice)
					case preparation == "failed":
						require.ErrorIs(t, err, fixture.engine.prepareErr)
					default:
						require.NoError(t, err)
					}
				}
				prepared := fixture.handler.preparedPayload.payloads[fixture.targetSlot]
				if preparation == "mismatch" {
					fixture.handler.validatorParams.SetFeeRecipient(fixture.proposer, common.Address{0x43})
					if needsForkChoice {
						fixture.handler.preparedPayload.set(fixture.targetSlot, []byte{0xff}, fixture.baseRoot, time.Now().Add(-time.Second))
					}
				}
				reused := preparation == "prepared" && !needsForkChoice
				if reused {
					time.Sleep(maximumPreparedAdvance(&fixture.cfg, fixture.version))
				}
				fixture.slotStart = time.Now()
				normal := computeBlockBuilderWindow(fixture.slotStart, fixture.slotStart, &fixture.cfg, fixture.version, 0)
				block, err := fixture.handler.produceBlock(t.Context(), 3, fixture.currentSlot, fixture.baseRoot, fixture.productionState, fixture.targetSlot, common.Bytes96{0xc0}, common.Hash{})
				require.NoError(t, err)
				require.NotNil(t, block)
				payload := block.BeaconBody.ExecutionPayload
				if fixture.version >= clparams.GloasVersion {
					built, ok := fixture.handler.selfBuildPayloads.Get(block.BeaconBody.SignedExecutionPayloadBid.Message.BlockHash)
					require.True(t, ok)
					payload = built.Payload
				}
				wantParent := fixture.parentHash
				if path.wantParentFull {
					wantParent = fixture.fullHash
				}
				require.Equal(t, wantParent, payload.ParentHash)
				require.Equal(t, fixture.currentSlot, fixture.headState.Slot(), "preparation must only advance its copied state")
				require.Equal(t, fixture.engine.productionID, fixture.engine.collectedID)
				if reused {
					require.Equal(t, prepared.id, fixture.engine.collectedID)
					require.True(t, fixture.engine.firstCollection.Before(normal.firstGetAt), "prepared collection must start before the normal window")
				} else {
					require.False(t, fixture.engine.firstCollection.Before(normal.firstGetAt), "ordinary collection timing must be retained")
					if len(prepared.id) > 0 {
						require.NotEqual(t, prepared.id, fixture.engine.collectedID)
					}
				}
			})
		}
	}
}
