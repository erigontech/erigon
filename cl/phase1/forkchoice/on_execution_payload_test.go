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
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
)

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
