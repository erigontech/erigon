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

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	syncmock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

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

func TestPayloadPreparationCollectionWithExecutionModule(t *testing.T) {
	for _, path := range []string{"ordinary", "epoch", "first_gloas", "empty", "full", "full_to_empty"} {
		for _, preparation := range []string{"prepared", "absent", "failed", "mismatch"} {
			t.Run(path+"/"+preparation, func(t *testing.T) {
				_, _, _, _, fixture, handler, _, synced, store, validators := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
				cfg := *handler.beaconChainCfg
				cfg.SecondsPerSlot = 4
				currentEpoch := fixture.Slot() / cfg.SlotsPerEpoch
				if path == "first_gloas" {
					cfg.FuluForkEpoch = currentEpoch
					cfg.GloasForkEpoch = currentEpoch + 1
				} else if path == "empty" || path == "full" || path == "full_to_empty" {
					cfg.FuluForkEpoch = currentEpoch
					cfg.GloasForkEpoch = currentEpoch
				}
				cfg.InitializeForkSchedule()
				handler.beaconChainCfg = &cfg
				headState := state.New(&cfg)
				require.NoError(t, fixture.CopyInto(headState))
				if cfg.FuluForkEpoch == currentEpoch {
					require.NoError(t, headState.UpgradeToFulu())
				}
				if cfg.GloasForkEpoch == currentEpoch {
					require.NoError(t, headState.UpgradeToGloas())
				}
				if path == "epoch" || path == "first_gloas" {
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
				} else if path == "first_gloas" {
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
				if path == "full_to_empty" {
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
				if path == "full" || path == "full_to_empty" {
					store.HeadPayloadStatusVal = cltypes.PayloadStatusFull
					buildOnFull := path == "full"
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
				slotStart := time.Now().Add(4 * time.Second)
				clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
				clock.EXPECT().GetSlotTime(targetSlot).DoAndReturn(func(uint64) time.Time { return slotStart }).AnyTimes()
				handler.ethClock = clock
				if preparation == "failed" {
					engine.prepareErr = errors.New("temporary execution failure")
				}
				needsFCU := path == "full" || path == "full_to_empty"
				if preparation != "absent" {
					_, err = preparePayloadForTest(t, handler, targetSlot)
					switch {
					case needsFCU:
						require.ErrorIs(t, err, errGloasPathNeedsForkChoice)
					case preparation == "failed":
						require.ErrorIs(t, err, engine.prepareErr)
					default:
						require.NoError(t, err)
					}
				}
				prepared := handler.preparedPayload.payloads[targetSlot]
				if preparation == "mismatch" {
					validators.SetFeeRecipient(proposer, common.Address{0x43})
					if needsFCU {
						handler.preparedPayload.set(targetSlot, []byte{0xff}, baseRoot, time.Now().Add(-time.Second))
					}
				}
				reused := preparation == "prepared" && !needsFCU
				if reused {
					time.Sleep(maximumPreparedAdvance(&cfg, version))
				}
				slotStart = time.Now()
				normal := computeBlockBuilderWindow(slotStart, slotStart, &cfg, version, 0)
				block, err := handler.produceBlock(t.Context(), 3, currentSlot, baseRoot, productionState, targetSlot, common.Bytes96{0xc0}, common.Hash{})
				require.NoError(t, err)
				require.NotNil(t, block)
				payload := block.BeaconBody.ExecutionPayload
				if version >= clparams.GloasVersion {
					built, ok := handler.selfBuildPayloads.Get(block.BeaconBody.SignedExecutionPayloadBid.Message.BlockHash)
					require.True(t, ok)
					payload = built.Payload
				}
				wantParent := parent.Hash()
				if path == "full" {
					wantParent = full.Hash()
				}
				require.Equal(t, wantParent, payload.ParentHash)
				require.Equal(t, currentSlot, headState.Slot(), "preparation must only advance its copied state")
				require.Equal(t, engine.productionID, engine.collectedID)
				if reused {
					require.Equal(t, prepared.id, engine.collectedID)
					require.True(t, engine.firstCollection.Before(normal.firstGetAt), "prepared collection must start before the normal window")
				} else {
					require.False(t, engine.firstCollection.Before(normal.firstGetAt), "ordinary collection timing must be retained")
					if len(prepared.id) > 0 {
						require.NotEqual(t, prepared.id, engine.collectedID)
					}
				}
			})
		}
	}
}
