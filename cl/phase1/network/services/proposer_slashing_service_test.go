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
	"errors"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type proposerSlashingTestSuite struct {
	suite.Suite
	gomockCtrl              *gomock.Controller
	operationsPool          *pool.OperationsPool
	syncedData              synced_data.SyncedData
	beaconCfg               *clparams.BeaconChainConfig
	ethClock                *eth_clock.MockEthereumClock
	proposerSlashingService *proposerSlashingService
	mockFuncs               *mockFuncs
}

func (t *proposerSlashingTestSuite) SetupTest() {
	saveSignatureGlobals(t.T())
	t.gomockCtrl = gomock.NewController(t.T())
	t.operationsPool = &pool.OperationsPool{
		ProposerSlashingsPool: pool.NewOperationPool[common.Bytes96, *cltypes.ProposerSlashing](10, "proposerSlashingsPool"),
	}
	_, st, _ := tests.GetBellatrixRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.Require().NoError(t.syncedData.OnHeadState(st))
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconCfg = &clparams.BeaconChainConfig{
		SlotsPerEpoch: 2,
	}
	emitters := beaconevents.NewEventEmitter()
	t.proposerSlashingService = NewProposerSlashingService(*t.operationsPool, t.syncedData, t.beaconCfg, t.ethClock, emitters)
	// mock global functions
	t.mockFuncs = &mockFuncs{ctrl: t.gomockCtrl}
	computeSigningRoot = t.mockFuncs.ComputeSigningRoot
	blsVerify = t.mockFuncs.BlsVerify
}

func (t *proposerSlashingTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func (t *proposerSlashingTestSuite) TestProcessMessage() {
	mockProposerIndex := uint64(123)
	mockMsg := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: mockProposerIndex,
				Root:          common.Hash{1},
			},
			Signature: common.Bytes96{1, 2, 3},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: mockProposerIndex,
				Root:          common.Hash{2},
			},
			Signature: common.Bytes96{4, 5, 6},
		},
	}
	mockMsg2 := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: 9191991,
				Root:          common.Hash{1},
			},
			Signature: common.Bytes96{1, 2, 3},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: 9191991,
				Root:          common.Hash{2},
			},
			Signature: common.Bytes96{4, 5, 6},
		},
	}
	msgWithPooledSignaturesOnInvalidHeaders := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: mockProposerIndex,
			},
			Signature: mockMsg.Header1.Signature,
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          2,
				ProposerIndex: mockProposerIndex,
			},
			Signature: mockMsg.Header2.Signature,
		},
	}
	tests := []struct {
		name     string
		mock     func()
		msg      *cltypes.ProposerSlashing
		wantErr  bool
		errIs    error
		errIsNot error
	}{
		{
			name: "ignore proposer slashing",
			mock: func() {
				t.proposerSlashingService.seenProposers.Store(mockProposerIndex, struct{}{})
			},
			msg:     mockMsg,
			wantErr: true,
			errIs:   ErrIgnore,
		},
		{
			name: "reject invalid headers with signatures already in pool",
			mock: func() {
				t.operationsPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(mockMsg), mockMsg)
			},
			msg:      msgWithPooledSignaturesOnInvalidHeaders,
			wantErr:  true,
			errIsNot: ErrIgnore,
		},
		{
			name: "non-matching slots",
			mock: func() {},
			msg: &cltypes.ProposerSlashing{
				Header1: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: mockProposerIndex,
					},
				},
				Header2: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          2,
						ProposerIndex: mockProposerIndex,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "non-matching proposer indices",
			mock: func() {},
			msg: &cltypes.ProposerSlashing{
				Header1: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: mockProposerIndex,
					},
				},
				Header2: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: mockProposerIndex + 1,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty head state",
			mock: func() {
				t.syncedData.UnsetHeadState()
			},
			msg:     mockMsg,
			wantErr: true,
			errIs:   synced_data.ErrNotSynced,
		},
		{
			name: "validator not found",
			mock: func() {
				_, st, _ := tests.GetBellatrixRandom()
				t.Require().NoError(t.syncedData.OnHeadState(st))
			},
			msg:     mockMsg2,
			wantErr: true,
		},
		{
			name: "proposer is not slashable",
			mock: func() {
				mockValidator := solid.NewValidatorFromParameters(
					[48]byte{},
					[32]byte{},
					0,
					false,
					0,
					0,
					0,
					0,
				)
				_, st, _ := tests.GetBellatrixRandom()
				st.ValidatorSet().Set(int(mockProposerIndex), mockValidator)
				t.Require().NoError(t.syncedData.OnHeadState(st))

				t.ethClock.EXPECT().GetCurrentEpoch().Return(uint64(1)).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "pass",
			mock: func() {
				// mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				// mockValidator := solid.NewValidatorFromParameters(
				// 	[48]byte{},
				// 	[32]byte{},
				// 	0,
				// 	false,
				// 	0,
				// 	0,
				// 	2,
				// 	2,
				// )
				t.ethClock.EXPECT().GetCurrentEpoch().Return(uint64(1)).Times(1)

				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.Header1.Header, gomock.Any()).Return([32]byte{}, nil).Times(1)
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.Header2.Header, gomock.Any()).Return([32]byte{}, nil).Times(1)
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(2)
			},
			msg:     mockMsg,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		log.Printf("Running test case: %s", tt.name)
		t.SetupTest()
		tt.mock()
		err := t.proposerSlashingService.ProcessMessage(context.Background(), nil, tt.msg)
		if tt.wantErr {
			t.Error(err)
			if tt.errIs != nil {
				t.ErrorIs(err, tt.errIs)
			}
			if tt.errIsNot != nil {
				t.NotErrorIs(err, tt.errIsNot)
			}
		} else {
			t.NoError(err)
		}
		t.gomockCtrl.Satisfied()
	}
}

func TestProposerSlashing(t *testing.T) {
	suite.Run(t, new(proposerSlashingTestSuite))
}

func TestProposerSlashingServiceAcceptsSpecSignedHeaders(t *testing.T) {
	service, operationsPool, proposerIndex, signedSlashing := newSpecSignedProposerSlashingTest(t, 1)
	msg := signedSlashing(proposerIndex, common.Hash{1}, common.Hash{2})

	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	require.True(t, operationsPool.ProposerSlashingsPool.Has(pool.ComputeKeyForProposerSlashing(msg)))
}

func TestProposerSlashingServiceIgnoresMalformedAfterAccepted(t *testing.T) {
	service, operationsPool, proposerIndex, signedSlashing := newSpecSignedProposerSlashingTest(t, 1)
	valid := signedSlashing(proposerIndex, common.Hash{1}, common.Hash{2})
	require.NoError(t, service.ProcessMessage(context.Background(), nil, valid))

	malformed := signedSlashing(proposerIndex, common.Hash{3}, common.Hash{4})
	malformed.Header2.Header.Slot++
	require.ErrorIs(t, service.ProcessMessage(context.Background(), nil, malformed), ErrIgnore)
	require.Len(t, operationsPool.ProposerSlashingsPool.Raw(), 1)
	require.True(t, operationsPool.ProposerSlashingsPool.Has(pool.ComputeKeyForProposerSlashing(valid)))
	require.False(t, operationsPool.ProposerSlashingsPool.Has(pool.ComputeKeyForProposerSlashing(malformed)))
}

func TestProposerSlashingServiceKeepsAcceptedProposerAfterManyOthers(t *testing.T) {
	const otherProposers = 100
	service, operationsPool, proposerIndex, signedSlashing := newSpecSignedProposerSlashingTest(t, otherProposers+1)
	first := signedSlashing(proposerIndex, common.Hash{1}, common.Hash{2})
	require.NoError(t, service.ProcessMessage(context.Background(), nil, first))

	for i := range otherProposers {
		msg := signedSlashing(proposerIndex+uint64(i)+1, common.Hash{3}, common.Hash{4})
		require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	}

	second := signedSlashing(proposerIndex, common.Hash{5}, common.Hash{6})
	require.ErrorIs(t, service.ProcessMessage(context.Background(), nil, second), ErrIgnore)
	require.Len(t, operationsPool.ProposerSlashingsPool.Raw(), otherProposers+1)
	require.False(t, operationsPool.ProposerSlashingsPool.Has(pool.ComputeKeyForProposerSlashing(second)))
}

func TestProposerSlashingServiceConcurrentValidationAcceptsOne(t *testing.T) {
	service, operationsPool, proposerIndex, signedSlashing := newSpecSignedProposerSlashingTest(t, 1)
	first := signedSlashing(proposerIndex, common.Hash{1}, common.Hash{2})
	second := signedSlashing(proposerIndex, common.Hash{3}, common.Hash{4})

	realBLSVerify := bls.Verify
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var verificationCalls atomic.Int32
	blsVerify = func(signature, message, publicKey []byte) (bool, error) {
		if verificationCalls.Add(1) <= 2 {
			arrived <- struct{}{}
			<-release
		}
		return realBLSVerify(signature, message, publicKey)
	}

	results := make(chan error, 2)
	go func() {
		results <- service.ProcessMessage(context.Background(), nil, first)
	}()
	go func() {
		results <- service.ProcessMessage(context.Background(), nil, second)
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range 2 {
		select {
		case <-arrived:
		case <-timer.C:
			close(release)
			<-results
			<-results
			t.Fatal("concurrent validations did not reach signature verification")
		}
	}
	close(release)

	accepted := 0
	ignored := 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrIgnore):
			ignored++
		default:
			t.Fatalf("unexpected validation error: %v", err)
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, ignored)
	require.Len(t, operationsPool.ProposerSlashingsPool.Raw(), 1)
}

func newSpecSignedProposerSlashingTest(t *testing.T, proposerCount int) (*proposerSlashingService, pool.OperationsPool, uint64, func(uint64, common.Hash, common.Hash) *cltypes.ProposerSlashing) {
	t.Helper()
	saveSignatureGlobals(t)
	computeSigningRoot = fork.ComputeSigningRoot
	blsVerify = bls.Verify

	_, st, _ := tests.GetBellatrixRandom()
	cfg := st.BeaconConfig()
	sk, err := bls.GenerateKey()
	require.NoError(t, err)
	var pubKey common.Bytes48
	copy(pubKey[:], bls.CompressPublicKey(sk.PublicKey()))
	proposerIndex := uint64(st.ValidatorLength())
	for range proposerCount {
		require.NoError(t, st.AddValidator(solid.NewValidatorFromParameters(pubKey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance))
	}

	signedHeader := func(proposerIndex uint64, root common.Hash) *cltypes.SignedBeaconBlockHeader {
		header := &cltypes.BeaconBlockHeader{Slot: st.Slot(), ProposerIndex: proposerIndex, Root: root}
		domain, err := st.GetDomain(cfg.DomainBeaconProposer, state.Epoch(st))
		require.NoError(t, err)
		signingRoot, err := fork.ComputeSigningRoot(header, domain)
		require.NoError(t, err)
		signed := &cltypes.SignedBeaconBlockHeader{Header: header}
		copy(signed.Signature[:], sk.Sign(signingRoot[:]).Bytes())
		return signed
	}
	syncedData := synced_data.NewSyncedDataManager(cfg, true)
	require.NoError(t, syncedData.OnHeadState(st))
	ethClock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	ethClock.EXPECT().GetCurrentEpoch().Return(state.Epoch(st)).AnyTimes()
	operationsPool := pool.OperationsPool{
		ProposerSlashingsPool: pool.NewOperationPool[common.Bytes96, *cltypes.ProposerSlashing](proposerCount+1, "proposerSlashingsPool"),
	}
	service := NewProposerSlashingService(operationsPool, syncedData, cfg, ethClock, beaconevents.NewEventEmitter())
	return service, operationsPool, proposerIndex, func(proposerIndex uint64, firstRoot, secondRoot common.Hash) *cltypes.ProposerSlashing {
		return &cltypes.ProposerSlashing{Header1: signedHeader(proposerIndex, firstRoot), Header2: signedHeader(proposerIndex, secondRoot)}
	}
}
