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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

func TestCaplinBlockProductionIntegration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
	}{
		{"Pectra", clparams.ElectraVersion},
		{"Glamsterdam", clparams.GloasVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			cfg := *chain.AllProtocolChanges
			if tc.version.Before(clparams.GloasVersion) {
				cfg.OsakaTime = nil
				cfg.AmsterdamTime = nil
			}
			producer := execmoduletester.New(t, execmoduletester.WithChainConfig(&cfg), execmoduletester.WithTxPool())
			receiver := execmoduletester.New(t, execmoduletester.WithChainConfig(&cfg))
			chainPack, err := producer.GenerateChain(1, nil)
			require.NoError(t, err)
			require.NoError(t, producer.InsertChain(chainPack))
			require.NoError(t, receiver.InsertChain(chainPack))
			parent := chainPack.TopBlock

			recipient := common.Address{0x42}
			value := uint256.NewInt(10_000)
			transfer, err := types.SignTx(
				types.NewTransaction(0, recipient, value, 1_000_000, producer.Genesis.BaseFee(), nil),
				*types.LatestSignerForChainID(cfg.ChainID), producer.Key,
			)
			require.NoError(t, err)
			var encodedTx bytes.Buffer
			require.NoError(t, transfer.EncodeRLP(&encodedTx))
			added, err := producer.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encodedTx.Bytes()}})
			require.NoError(t, err)
			require.Equal(t, []string{"success"}, added.Errors)

			_, blocks, _, _, postState, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
			handler.engine, err = execution_client.NewExecutionClientDirect(chainreader.NewChainReaderEth1(&cfg, producer.ExecModule, time.Minute), nil)
			require.NoError(t, err)
			payloadHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
			payloadHeader.BlockHash = parent.Hash()
			payloadHeader.BlockNumber = parent.NumberU64()
			payloadHeader.Time = parent.Time()
			postState.SetLatestExecutionPayloadHeader(payloadHeader)
			if tc.version.AfterOrEqual(clparams.GloasVersion) {
				handler.beaconChainCfg.FuluForkEpoch = 1
				handler.beaconChainCfg.GloasForkEpoch = 1
				handler.beaconChainCfg.InitializeForkSchedule()
				require.NoError(t, postState.UpgradeToFulu())
				require.NoError(t, postState.UpgradeToGloas())
			}
			baseBlock := blocks[len(blocks)-1].Block
			var baseRoot common.Hash
			baseRoot, err = baseBlock.HashSSZ()
			require.NoError(t, err)
			forkchoiceStore.HeadVal = baseRoot
			forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusFull
			forkchoiceStore.SetEnvelope(baseRoot, &cltypes.SignedExecutionPayloadEnvelope{
				Message: &cltypes.ExecutionPayloadEnvelope{
					ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion),
				},
			})
			targetSlot := baseBlock.Slot + 1
			require.NoError(t, transition.DefaultMachine.ProcessSlots(postState, targetSlot))

			body, blockValue, err := handler.produceBeaconBody(ctx, 3, baseBlock.Slot, baseRoot, postState, targetSlot, common.Bytes96{0xc0}, common.Hash{})
			require.NoError(t, err)
			require.Positive(t, blockValue.Sign())
			payload, requests := body.ExecutionPayload, body.ExecutionRequests
			if tc.version.AfterOrEqual(clparams.GloasVersion) {
				built, ok := handler.selfBuildPayloads.Get(body.SignedExecutionPayloadBid.Message.BlockHash)
				require.True(t, ok)
				payload, requests = built.Payload, built.ExecutionRequests
			}
			require.NotNil(t, payload)
			require.Equal(t, parent.Hash(), payload.ParentHash)
			require.Equal(t, parent.NumberU64()+1, payload.BlockNumber)
			require.Equal(t, [][]byte{encodedTx.Bytes()}, payload.Transactions.UnderlyngReference())

			receiverEngine, err := execution_client.NewExecutionClientDirect(chainreader.NewChainReaderEth1(&cfg, receiver.ExecModule, time.Minute), nil)
			require.NoError(t, err)
			status, err := receiverEngine.NewPayload(ctx, payload, &baseRoot, nil, cltypes.GetExecutionRequestsList(handler.beaconChainCfg, requests))
			require.NoError(t, err, "Caplin's block must be accepted by another execution layer")
			require.EqualValues(t, execution_client.PayloadStatusValidated, status)
			_, err = receiverEngine.ForkChoiceUpdate(ctx, parent.Hash(), parent.Hash(), payload.BlockHash, nil, tc.version)
			require.NoError(t, err)
			receiver.ExecModule.WaitIdle(ctx)
			head, err := receiverEngine.CurrentHeader(ctx)
			require.NoError(t, err)
			require.NotNil(t, head)
			require.Equal(t, payload.BlockHash, head.Hash())

			tx, err := receiver.DB.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			account, err := receiver.NewStateReader(tx).ReadAccountData(accounts.InternAddress(recipient))
			require.NoError(t, err)
			require.NotNil(t, account)
			require.Equal(t, *value, account.Balance)
		})
	}
}
