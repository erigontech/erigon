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
	"math/big"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
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
	head, executionHead := fixture.updateExecutionHead(t)
	require.Equal(t, forkchoice.ForkChoiceNode{Root: fixture.parentRoot, PayloadStatus: cltypes.PayloadStatusFull}, head)
	require.Equal(t, fixture.parentPayload.Payload.BlockHash, executionHead)

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
	store, err := forkchoice.NewForkChoiceStore(clock, anchor, engine, pool.NewOperationsPool(handler.beaconChainCfg), graph, beaconevents.NewEventEmitter(), synced, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(), handler.validatorParams, false, nil)
	require.NoError(t, err)
	handler.engine, handler.forkchoiceStore, handler.syncedData, handler.ethClock = engine, store, synced, clock
	require.NoError(t, synced.OnHeadStateWithBlockRoot(anchor, store.AnchorRoot()))

	parentState, parentBlock, parentEnvelope, parentPayload, parentRoot, parentSlot := produceGloasIntegrationChild(t, handler, anchor, store.AnchorRoot(), anchor.LatestBlockHeader().Slot)
	store.OnTick(utils.SecondsToMilliseconds(anchor.GenesisTime() + parentSlot*handler.beaconChainCfg.SecondsPerSlot))
	applyGossipIntegrationEnvelope(t, store, parentBlock, parentEnvelope)

	return &gloasPreparationIntegrationFixture{
		module: module, reader: reader, engine: engine, handler: handler, store: store, synced: synced,
		anchor: anchorTemplate, seedHash: seedHash, parentState: parentState, parentBlock: parentBlock,
		parentRoot: parentRoot, parentSlot: parentSlot, parentPayload: parentPayload, parentEnvelope: parentEnvelope,
	}
}

func (f *gloasPreparationIntegrationFixture) updateExecutionHead(t *testing.T) (forkchoice.ForkChoiceNode, common.Hash) {
	t.Helper()
	head, executionHead := updateGloasIntegrationExecutionHead(t, f.store, f.engine, f.handler.beaconChainCfg, f.parentSlot)
	f.module.ExecModule.WaitIdle(t.Context())
	require.NoError(t, f.synced.OnHeadStateWithBlockRoot(f.parentState, f.parentRoot))
	return head, executionHead
}

func updateGloasIntegrationExecutionHead(
	t *testing.T,
	store *forkchoice.ForkChoiceStore,
	engine execution_client.ExecutionEngine,
	beaconCfg *clparams.BeaconChainConfig,
	headSlot uint64,
) (forkchoice.ForkChoiceNode, common.Hash) {
	t.Helper()
	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	executionHead, ready := store.GetExecutionHead(head)
	require.True(t, ready)
	finalized := store.FinalizedCheckpoint()
	justified := store.JustifiedCheckpoint()
	_, err = engine.ForkChoiceUpdate(
		t.Context(), store.GetFinalizedExecutionHash(finalized.Root), store.GetFinalizedExecutionHash(justified.Root),
		executionHead, nil, beaconCfg.GetCurrentStateVersion(headSlot/beaconCfg.SlotsPerEpoch),
	)
	require.NoError(t, err)
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
	store *forkchoice.ForkChoiceStore,
	block *cltypes.SignedBeaconBlock,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
) {
	t.Helper()
	require.NoError(t, store.OnBlock(t.Context(), block, true, false, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	blockState, err := store.GetStateAtBlockRoot(root, false)
	require.NoError(t, err)
	require.NotNil(t, blockState)
	key, err := bls.GenerateKey()
	require.NoError(t, err)
	validator, err := blockState.ValidatorForValidatorIndex(int(block.Block.ProposerIndex))
	require.NoError(t, err)
	originalPublicKey := validator.PublicKey()
	validator.SetPublicKey(common.Bytes48(bls.CompressPublicKey(key.PublicKey())))
	blockState.SetValidatorAtIndex(int(block.Block.ProposerIndex), validator)
	signExecutionPayloadEnvelope(t, blockState, block.Block.Slot, envelope, key)
	require.NoError(t, store.OnExecutionPayload(t.Context(), envelope, false, true))
	validator.SetPublicKey(originalPublicKey)
	blockState.SetValidatorAtIndex(int(block.Block.ProposerIndex), validator)
}

func TestGloasCanonicalForkChoiceAdvancesExecutionHeadOverUnverifiedFullHead(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	fixture := newGloasPreparationIntegrationFixture(t)
	fixture.updateExecutionHead(t)
	childState, childBlock, childEnvelope, childPayload, childRoot, childSlot := produceGloasIntegrationChild(
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
	applyGossipIntegrationEnvelope(t, store, fixture.parentBlock, fixture.parentEnvelope)
	require.True(t, store.IsPayloadVerified(fixture.parentRoot))
	store.OnTick(utils.SecondsToMilliseconds(anchor.GenesisTime() + childSlot*fixture.handler.beaconChainCfg.SecondsPerSlot))
	applyGossipIntegrationEnvelope(t, store, childBlock, childEnvelope)
	require.False(t, store.IsPayloadVerified(childRoot))
	require.NoError(t, synced.OnHeadStateWithBlockRoot(childState, childRoot))

	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, forkchoice.ForkChoiceNode{Root: childRoot, PayloadStatus: cltypes.PayloadStatusFull}, head)
	before, _, _, err := importReader.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, childPayload.Payload.BlockHash, before)

	_, sent := updateGloasIntegrationExecutionHead(t, store, importEngine, fixture.handler.beaconChainCfg, childSlot)
	require.Equal(t, childPayload.Payload.BlockHash, sent)
	importModule.ExecModule.WaitIdle(t.Context())
	after, _, _, err := importReader.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.Equal(t, childPayload.Payload.BlockHash, after)
}

func TestGloasPreparationDiscardsBuildAfterPayloadInvalidation(t *testing.T) {
	fixture := newGloasPreparationIntegrationFixture(t)
	fixture.updateExecutionHead(t)
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
	fixture.updateExecutionHead(t)
	_, childBlock, _, _, _, childSlot := produceGloasIntegrationChild(
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

	require.ErrorIs(t, err, errForkChoiceHeadChanged)
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
