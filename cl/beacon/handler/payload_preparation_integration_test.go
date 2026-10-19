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
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
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
	forkChoiceCalls int
	productionID    []byte
	collectedID     []byte
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

func TestGloasFullParentPreparationWithExecutionModule(t *testing.T) {
	ctx := t.Context()
	module := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	seed, err := module.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, module.InsertChain(seed))
	reader := chainreader.NewChainReaderEth1(module.ChainConfig, module.ExecModule, time.Second)
	direct, err := execution_client.NewExecutionClientDirect(reader, nil)
	require.NoError(t, err)
	engine := &preparationExecutionClient{ExecutionClientDirect: direct}

	fixture, handler, _, _, validators := setupGloasPreparationTest(t)
	anchor := state.New(handler.beaconChainCfg)
	require.NoError(t, fixture.CopyInto(anchor))
	anchorSlot := (anchor.Slot()/handler.beaconChainCfg.SlotsPerEpoch + 1) * handler.beaconChainCfg.SlotsPerEpoch
	require.NoError(t, transition.DefaultMachine.ProcessSlots(anchor, anchorSlot))
	anchor.SetGenesisTime(uint64(time.Now().Unix()) - (anchorSlot+1)*handler.beaconChainCfg.SecondsPerSlot - 2)
	anchor.SetLatestBlockHash(seed.TopBlock.Hash())
	anchor.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{ParentBlockHash: seed.TopBlock.Hash(), GasLimit: seed.TopBlock.GasLimit()})
	clock := eth_clock.NewEthereumClock(anchor.GenesisTime(), anchor.GenesisValidatorsRoot(), handler.beaconChainCfg)
	synced := synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	graph, err := fork_graph.NewForkGraphDisk(anchor, synced, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	store, err := forkchoice.NewForkChoiceStore(clock, anchor, engine, pool.NewOperationsPool(handler.beaconChainCfg), graph, beaconevents.NewEventEmitter(), synced, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(), validators, false, nil)
	require.NoError(t, err)
	handler.engine, handler.forkchoiceStore, handler.syncedData, handler.ethClock = engine, store, synced, clock
	require.NoError(t, synced.OnHeadStateWithBlockRoot(anchor, store.AnchorRoot()))
	store.OnTick(utils.SecondsToMilliseconds(anchor.GenesisTime() + (anchorSlot+1)*handler.beaconChainCfg.SecondsPerSlot))
	_, _, err = store.GetHeadNode()
	require.NoError(t, err)

	parentState, err := anchor.Copy()
	require.NoError(t, err)
	parentSlot := anchorSlot + 1
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, parentSlot))
	parent, err := handler.produceBlock(ctx, 0, anchor.LatestBlockHeader().Slot, store.AnchorRoot(), parentState, parentSlot, common.Bytes96{0xc0}, common.Hash{})
	require.NoError(t, err)
	parentState, _, err = handler.processProducedBlock(parentState, parent)
	require.NoError(t, err)
	parent.StateRoot, err = parentState.HashSSZ()
	require.NoError(t, err)
	signedBlock := &cltypes.SignedBeaconBlock{Block: parent.ToExecution().Block}
	root, err := signedBlock.Block.HashSSZ()
	require.NoError(t, err)
	parentPayload, ok := handler.selfBuildPayloads.Get(parent.BeaconBody.SignedExecutionPayloadBid.Message.BlockHash)
	require.True(t, ok)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload: parentPayload.Payload, ExecutionRequests: parentPayload.ExecutionRequests,
		BuilderIndex: clparams.BuilderIndexSelfBuild, BeaconBlockRoot: root, ParentBeaconBlockRoot: signedBlock.Block.ParentRoot,
	}}

	require.NoError(t, store.OnBlock(ctx, signedBlock, true, false, false))
	require.NoError(t, store.ApplyLocalSelfBuildEnvelope(ctx, envelope))
	require.True(t, store.IsPayloadVerified(root))
	executionHead, _, _, err := reader.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, seed.TopBlock.Hash(), executionHead, "NewPayload must leave fork choice at the EMPTY parent")
	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, forkchoice.ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusFull}, head)
	executionHead, err = store.UpdateExecutionHead(ctx, head)
	require.NoError(t, err)
	require.Equal(t, parentPayload.Payload.BlockHash, executionHead)
	module.ExecModule.WaitIdle(ctx)
	require.NoError(t, synced.OnHeadStateWithBlockRoot(parentState, root))

	targetSlot := parentSlot + 1
	proposer, err := parentState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validators.SetFeeRecipient(proposer, common.Address{0x42})
	callsBeforePreparation := engine.forkChoiceCalls
	preparationCtx, cancel := context.WithCancel(ctx)
	done := handler.StartPayloadPreparation(preparationCtx)
	t.Cleanup(func() { cancel(); <-done })
	var prepared preparedPayloadRecord
	require.Eventually(t, func() bool {
		handler.preparedPayload.mu.Lock()
		defer handler.preparedPayload.mu.Unlock()
		prepared = handler.preparedPayload.payloads[targetSlot]
		return len(prepared.id) != 0
	}, time.Until(clock.GetSlotTime(targetSlot)), 10*time.Millisecond)
	cancel()
	<-done
	require.Equal(t, callsBeforePreparation, engine.forkChoiceCalls)
	require.NotEmpty(t, prepared.id)
	require.True(t, prepared.primedAt.Before(clock.GetSlotTime(targetSlot)))
	require.NoError(t, common.Sleep(ctx, time.Until(clock.GetSlotTime(targetSlot))))
	warmup := time.Since(prepared.primedAt)
	require.Greater(t, warmup, minimumPreparationLead)
	t.Logf("FULL-parent builder warmup before production: %s", warmup)

	productionState, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(productionState, targetSlot))
	body, _, err := handler.produceBeaconBody(ctx, parentSlot, root, productionState, targetSlot, common.Bytes96{0xc0}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, parentPayload.Payload.BlockHash, body.SignedExecutionPayloadBid.Message.ParentBlockHash)
	require.Equal(t, prepared.id, engine.productionID)
	require.Equal(t, prepared.id, engine.collectedID)
	_, _, mismatch := handler.preparedPayload.warmupAndMismatch(targetSlot, engine.productionID, time.Now())
	require.False(t, mismatch)
}
