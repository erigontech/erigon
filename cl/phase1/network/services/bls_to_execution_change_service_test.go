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

package services

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type blsToExecutionChangeTestContext struct {
	gomockCtrl     *gomock.Controller
	operationsPool *pool.OperationsPool
	syncedData     *synced_data.SyncedDataManager
	beaconCfg      *clparams.BeaconChainConfig

	service   BLSToExecutionChangeService
	mockFuncs *mockFuncs
}

func setupBLSToExecutionChangeTest(t *testing.T) (*blsToExecutionChangeTestContext, *state.CachingBeaconState) {
	t.Helper()

	gomockCtrl := gomock.NewController(t)
	t.Cleanup(gomockCtrl.Finish)

	operationsPool := &pool.OperationsPool{
		BLSToExecutionChangesPool: pool.NewOperationPool[common.Bytes96, *cltypes.SignedBLSToExecutionChange](10, "blsToExecutionChangesPool"),
	}
	_, st, _ := tests.GetCapellaRandom()

	syncedData := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, syncedData.OnHeadState(st))

	verifierCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batchSignatureVerifier := NewBatchSignatureVerifier(verifierCtx, nil)
	go batchSignatureVerifier.Start()

	mockFuncs := &mockFuncs{
		ctrl: gomockCtrl,
	}

	prevComputeSigningRoot := computeSigningRoot
	prevBlsVerifyMultipleSignatures := blsVerifyMultipleSignatures
	computeSigningRoot = mockFuncs.ComputeSigningRoot
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures
	t.Cleanup(func() {
		computeSigningRoot = prevComputeSigningRoot
		blsVerifyMultipleSignatures = prevBlsVerifyMultipleSignatures
	})

	return &blsToExecutionChangeTestContext{
		gomockCtrl:     gomockCtrl,
		operationsPool: operationsPool,
		syncedData:     syncedData,
		beaconCfg:      &clparams.MainnetBeaconConfig,
		service:        NewBLSToExecutionChangeService(*operationsPool, beaconevents.NewEventEmitter(), syncedData, &clparams.MainnetBeaconConfig, batchSignatureVerifier),
		mockFuncs:      mockFuncs,
	}, st
}

func newSignedBLSToExecutionChangeForGossip(validatorIndex uint64) *SignedBLSToExecutionChangeForGossip {
	return &SignedBLSToExecutionChangeForGossip{
		SignedBLSToExecutionChange: &cltypes.SignedBLSToExecutionChange{
			Message: &cltypes.BLSToExecutionChange{
				ValidatorIndex: validatorIndex,
				From:           common.Bytes48{1, 2, 3, 4, 5, 6},
				To:             common.Address{3, 2, 1},
			},
			Signature: [96]byte{1, 2, 3},
		},
		ImmediateVerification: true,
	}
}

func syncHeadState(t *testing.T, syncedData *synced_data.SyncedDataManager, st *state.CachingBeaconState) {
	t.Helper()
	require.NoError(t, syncedData.OnHeadState(st))
}

func matchingWithdrawalCredentials(cfg *clparams.BeaconChainConfig, from common.Bytes48) common.Hash {
	hashedFrom := utils.Sha256(from[:])
	wc := common.Hash{byte(cfg.BLSWithdrawalPrefixByte)}
	copy(wc[1:], hashedFrom[1:])
	return wc
}

func TestBlsToExecutionChangeProcessMessage(t *testing.T) {
	const validatorIndex = uint64(1)

	tests := []struct {
		name          string
		prepare       func(t *testing.T, testCtx *blsToExecutionChangeTestContext, st *state.CachingBeaconState, msg *SignedBLSToExecutionChangeForGossip)
		wantErr       error
		wantErrString string
		wantInPool    bool
	}{
		{
			name: "signature already exists in pool",
			prepare: func(t *testing.T, testCtx *blsToExecutionChangeTestContext, _ *state.CachingBeaconState, msg *SignedBLSToExecutionChangeForGossip) {
				testCtx.operationsPool.BLSToExecutionChangesPool.Insert(msg.SignedBLSToExecutionChange.Signature, msg.SignedBLSToExecutionChange)
			},
			wantInPool: true,
		},
		{
			name: "version is less than CapellaVersion",
			prepare: func(t *testing.T, testCtx *blsToExecutionChangeTestContext, _ *state.CachingBeaconState, _ *SignedBLSToExecutionChangeForGossip) {
				_, bellatrixState, _ := tests.GetBellatrixRandom()
				syncHeadState(t, testCtx.syncedData, bellatrixState)
			},
			wantErr: ErrIgnore,
		},
		{
			name: "unable to retrieve validator",
			prepare: func(t *testing.T, _ *blsToExecutionChangeTestContext, st *state.CachingBeaconState, msg *SignedBLSToExecutionChangeForGossip) {
				msg.SignedBLSToExecutionChange.Message.ValidatorIndex = uint64(st.ValidatorLength())
			},
			wantErrString: "unable to retrieve validator: invalid validator index",
		},
		{
			name: "invalid withdrawal credentials prefix",
			prepare: func(t *testing.T, testCtx *blsToExecutionChangeTestContext, st *state.CachingBeaconState, msg *SignedBLSToExecutionChangeForGossip) {
				wc := matchingWithdrawalCredentials(testCtx.beaconCfg, msg.SignedBLSToExecutionChange.Message.From)
				wc[0] = byte(testCtx.beaconCfg.ETH1AddressWithdrawalPrefixByte)
				st.ValidatorSet().SetWithdrawalCredentialForValidatorAtIndex(int(validatorIndex), wc)
				syncHeadState(t, testCtx.syncedData, st)
			},
			wantErrString: "invalid withdrawal credentials prefix",
		},
		{
			name: "hashed from is not equal to withdrawal credentials",
			prepare: func(t *testing.T, testCtx *blsToExecutionChangeTestContext, st *state.CachingBeaconState, _ *SignedBLSToExecutionChangeForGossip) {
				st.ValidatorSet().SetWithdrawalCredentialForValidatorAtIndex(int(validatorIndex), common.Hash{byte(testCtx.beaconCfg.BLSWithdrawalPrefixByte)})
				syncHeadState(t, testCtx.syncedData, st)
			},
			wantErrString: "invalid withdrawal credentials hash",
		},
		{
			name: "invalid bls signature",
			prepare: func(t *testing.T, testCtx *blsToExecutionChangeTestContext, st *state.CachingBeaconState, msg *SignedBLSToExecutionChangeForGossip) {
				st.ValidatorSet().SetWithdrawalCredentialForValidatorAtIndex(int(validatorIndex), matchingWithdrawalCredentials(testCtx.beaconCfg, msg.SignedBLSToExecutionChange.Message.From))
				syncHeadState(t, testCtx.syncedData, st)
				testCtx.gomockCtrl.RecordCall(testCtx.mockFuncs, "ComputeSigningRoot", msg.SignedBLSToExecutionChange.Message, gomock.Any()).Return([32]byte{}, nil).Times(1)
				testCtx.gomockCtrl.RecordCall(testCtx.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(2)
			},
			wantErr: ErrInvalidBlsSignature,
		},
		{
			name: "success",
			prepare: func(t *testing.T, testCtx *blsToExecutionChangeTestContext, st *state.CachingBeaconState, msg *SignedBLSToExecutionChangeForGossip) {
				st.ValidatorSet().SetWithdrawalCredentialForValidatorAtIndex(int(validatorIndex), matchingWithdrawalCredentials(testCtx.beaconCfg, msg.SignedBLSToExecutionChange.Message.From))
				syncHeadState(t, testCtx.syncedData, st)
				testCtx.gomockCtrl.RecordCall(testCtx.mockFuncs, "ComputeSigningRoot", msg.SignedBLSToExecutionChange.Message, gomock.Any()).Return([32]byte{}, nil).Times(1)
				testCtx.gomockCtrl.RecordCall(testCtx.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
			},
			wantInPool: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx, st := setupBLSToExecutionChangeTest(t)
			msg := newSignedBLSToExecutionChangeForGossip(validatorIndex)

			tt.prepare(t, testCtx, st, msg)

			err := testCtx.service.ProcessMessage(context.Background(), nil, msg)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else if tt.wantErrString != "" {
				require.ErrorContains(t, err, tt.wantErrString)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantInPool, testCtx.operationsPool.BLSToExecutionChangesPool.Has(msg.SignedBLSToExecutionChange.Signature))
			require.True(t, testCtx.gomockCtrl.Satisfied())
		})
	}
}
