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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/gossip"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

func TestCaplinBlockProductionIntegration(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg)
	}
	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
	}{
		{"Pectra", clparams.ElectraVersion},
		{"Fusaka", clparams.FuluVersion},
		{"Glamsterdam", clparams.GloasVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			cfg := *chain.AllProtocolChanges
			if tc.version.Before(clparams.FuluVersion) {
				cfg.OsakaTime = nil
			}
			if tc.version.Before(clparams.GloasVersion) {
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
			var blobTx *types.BlobTxWrapper
			if tc.version.Before(clparams.FuluVersion) {
				blobTx = types.MakeWrappedBlobTxn(cfg.ChainID)
			} else {
				blobTx = types.MakeV1WrappedBlobTxn(cfg.ChainID)
			}
			blobTx.Tx.Nonce = 1
			blobTx.Tx.To = &recipient
			signedBlobTx, err := types.SignTx(blobTx, *types.LatestSignerForChainID(cfg.ChainID), producer.Key)
			require.NoError(t, err)
			blobTx = signedBlobTx.(*types.BlobTxWrapper)
			var encodedBlobTx bytes.Buffer
			require.NoError(t, blobTx.MarshalBinaryWrapped(&encodedBlobTx))
			added, err := producer.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encodedTx.Bytes(), encodedBlobTx.Bytes()}})
			require.NoError(t, err)
			require.Equal(t, []string{"success", "success"}, added.Errors)

			_, blocks, _, _, postState, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
			handler.engine, err = execution_client.NewExecutionClientDirect(chainreader.NewChainReaderEth1(&cfg, producer.ExecModule, time.Minute), nil)
			require.NoError(t, err)
			payloadHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
			payloadHeader.BlockHash = parent.Hash()
			payloadHeader.BlockNumber = parent.NumberU64()
			payloadHeader.GasLimit = parent.GasLimit()
			payloadHeader.Time = parent.Time()
			postState.SetLatestExecutionPayloadHeader(payloadHeader)
			if tc.version.AfterOrEqual(clparams.FuluVersion) {
				handler.beaconChainCfg.FuluForkEpoch = 1
				require.NoError(t, postState.UpgradeToFulu())
			}
			if tc.version.AfterOrEqual(clparams.GloasVersion) {
				handler.beaconChainCfg.GloasForkEpoch = 1
				require.NoError(t, postState.UpgradeToGloas())
			}
			handler.beaconChainCfg.InitializeForkSchedule()
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

			block, err := handler.produceBlock(ctx, 3, baseBlock.Slot, baseRoot, postState, targetSlot, common.Bytes96{0xc0}, common.Hash{})
			require.NoError(t, err)
			require.False(t, block.IsBlinded())
			require.Positive(t, block.ExecutionValue.Sign())
			body := block.BeaconBody
			payload, requests := body.ExecutionPayload, body.ExecutionRequests
			if tc.version.AfterOrEqual(clparams.GloasVersion) {
				built, ok := handler.selfBuildPayloads.Get(body.SignedExecutionPayloadBid.Message.BlockHash)
				require.True(t, ok)
				payload, requests = built.Payload, built.ExecutionRequests
			}
			require.NotNil(t, payload)
			require.Equal(t, parent.Hash(), payload.ParentHash)
			require.Equal(t, parent.NumberU64()+1, payload.BlockNumber)
			transactions, err := types.MarshalTransactionsBinary([]types.Transaction{transfer, blobTx})
			require.NoError(t, err)
			require.Equal(t, transactions, payload.Transactions.UnderlyngReference())
			require.Equal(t, blobTx.GetBlobGas(), payload.BlobGasUsed)
			require.Len(t, block.Blobs, len(blobTx.Blobs))
			require.Len(t, block.KzgProofs, len(blobTx.Proofs))
			commitments := body.GetBlobKzgCommitments()
			require.Equal(t, len(blobTx.Commitments), commitments.Len())
			for i := range blobTx.Blobs {
				require.Equal(t, cltypes.Blob(blobTx.Blobs[i]), *block.Blobs[i])
				require.Equal(t, cltypes.KZGCommitment(blobTx.Commitments[i]), *commitments.Get(i))
			}

			receiverEngine, err := execution_client.NewExecutionClientDirect(chainreader.NewChainReaderEth1(&cfg, receiver.ExecModule, time.Minute), nil)
			require.NoError(t, err)
			status, err := receiverEngine.NewPayload(ctx, payload, &baseRoot, blobTx.GetBlobHashes(), cltypes.GetExecutionRequestsList(handler.beaconChainCfg, requests))
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

			requireCaplinPublishesBlobData(t, block, payload, requests)
		})
	}
}

func requireCaplinPublishesBlobData(t *testing.T, block *cltypes.BlindOrExecutionBeaconBlock, payload *cltypes.Eth1Block, requests *cltypes.ExecutionRequests) {
	t.Helper()
	version := block.Version()
	publisher, published := newPublishingHandler(t, version, nil)
	contents := block.ToExecution()
	signed := &cltypes.DenebSignedBeaconBlock{
		SignedBlock: &cltypes.SignedBeaconBlock{Block: contents.Block},
		Blobs:       contents.Blobs,
		KZGProofs:   contents.KZGProofs,
	}
	commitments := block.BeaconBody.GetBlobKzgCommitments()
	for i := 0; i < commitments.Len(); i++ {
		_, cached := publisher.blobBundles.Get(common.Bytes48(*commitments.Get(i)))
		require.False(t, cached)
	}
	blockRoot, err := contents.Block.HashSSZ()
	require.NoError(t, err)
	if version.Before(clparams.GloasVersion) {
		require.NoError(t, postBlock(t, publisher, signed, version, false))
	} else {
		publisher.beaconChainCfg.GloasForkEpoch = 1
		publisher.beaconChainCfg.InitializeForkSchedule()
		encoded, err := signed.SignedBlock.EncodeSSZ(nil)
		require.NoError(t, err)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(encoded))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Eth-Consensus-Version", version.String())
		_, err = publisher.PostEthV2BeaconBlocks(httptest.NewRecorder(), req)
		require.NoError(t, err)

		publisher.forkchoiceStore.(*forkchoice_mock.ForkChoiceStorageMock).Blocks[common.Hash(blockRoot)] = signed.SignedBlock
		publisher.columnStorage.(*blob_storage_mock.MockDataColumnStorage).EXPECT().
			WriteColumnSidecars(gomock.Any(), common.Hash(blockRoot), gomock.Any(), gomock.Any()).
			Return(nil).Times(int(publisher.beaconChainCfg.NumberOfColumns))
		publisher.sentinel = &nonNilSentinelClient{}
		envelopeContents := cltypes.NewSignedExecutionPayloadEnvelopeContents(publisher.beaconChainCfg, block.Slot)
		envelope := envelopeContents.SignedExecutionPayloadEnvelope.Message
		envelope.Payload = payload
		envelope.ExecutionRequests = requests
		envelope.BuilderIndex = block.BeaconBody.SignedExecutionPayloadBid.Message.BuilderIndex
		envelope.BeaconBlockRoot = common.Hash(blockRoot)
		envelope.ParentBeaconBlockRoot = block.ParentRoot
		envelopeContents.Blobs = contents.Blobs
		envelopeContents.KZGProofs = contents.KZGProofs
		encoded, err = envelopeContents.EncodeSSZ(nil)
		require.NoError(t, err)
		req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(encoded))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Eth-Consensus-Version", version.String())
		req.Header.Set("Eth-Blob-Data-Included", "true")
		recorder := httptest.NewRecorder()
		publisher.PostEthV1BeaconExecutionPayloadEnvelope(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		require.Contains(t, published.topics, gossip.TopicNameExecutionPayload)
	}
	require.Contains(t, published.topics, gossip.TopicNameBeaconBlock)
	if version.Before(clparams.FuluVersion) {
		for i, blob := range block.Blobs {
			var sidecar cltypes.BlobSidecar
			require.NoError(t, sidecar.DecodeSSZ(published.topics[gossip.TopicNameBlobSidecar(uint64(i))], int(version)))
			require.Equal(t, *blob, sidecar.Blob)
			require.Equal(t, common.Bytes48(*commitments.Get(i)), sidecar.KzgCommitment)
			require.NoError(t, kzg.Ctx().VerifyBlobKZGProof((*goethkzg.Blob)(&sidecar.Blob), goethkzg.KZGCommitment(sidecar.KzgCommitment), goethkzg.KZGProof(sidecar.KzgProof)))
			require.True(t, cltypes.VerifyCommitmentInclusionProof(sidecar.KzgCommitment, sidecar.CommitmentInclusionProof, sidecar.Index, sidecar.SignedBlockHeader.Header.BodyRoot))
			headerRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, blockRoot, headerRoot)
		}
		return
	}
	columns := 0
	for topic, data := range published.topics {
		if !strings.HasPrefix(topic, "data_column_sidecar_") {
			continue
		}
		columns++
		sidecar := cltypes.NewDataColumnSidecarWithVersion(version)
		require.NoError(t, sidecar.DecodeSSZ(data, int(version)))
		require.True(t, das.VerifyDataColumnSidecarKZGProofsWithCommitments(sidecar, commitments), "column %d", sidecar.Index)
		if version.Before(clparams.GloasVersion) {
			require.True(t, das.VerifyDataColumnSidecarInclusionProof(sidecar))
			headerRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, blockRoot, headerRoot)
		} else {
			require.Equal(t, common.Hash(blockRoot), sidecar.BeaconBlockRoot)
			require.Equal(t, block.Slot, sidecar.Slot)
		}
	}
	require.Equal(t, int(publisher.beaconChainCfg.NumberOfColumns), columns)
}
