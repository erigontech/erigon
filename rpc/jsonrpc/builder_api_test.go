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

package jsonrpc

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider/privatepool"
)

type recordingPrivateBundleAdmission struct {
	bundle privatepool.Bundle
	err    error
	calls  int
}

func (s *recordingPrivateBundleAdmission) Submit(_ context.Context, bundle privatepool.Bundle, _ *PrivateBundleSimulationResult) (common.Hash, error) {
	s.calls++
	s.bundle = bundle
	return bundle.Transaction.Hash(), s.err
}

type recordingPrivateBundleSimulator struct {
	targetHash common.Hash
	txn        types.Transaction
	result     *PrivateBundleSimulationResult
	err        error
}

func (s *recordingPrivateBundleSimulator) SimulatePrivateBundle(_ context.Context, targetHash common.Hash, txn types.Transaction, _ uint64) (*PrivateBundleSimulationResult, error) {
	s.targetHash = targetHash
	s.txn = txn
	return s.result, s.err
}

func signedPrivateTransaction(t *testing.T) (types.Transaction, hexutil.Bytes) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn, err := types.SignTx(
		types.NewTransaction(7, common.Address{0x44}, uint256.NewInt(3), 50_000, uint256.NewInt(2), []byte{0xaa}),
		*types.LatestSigner(chain.AllProtocolChanges),
		key,
	)
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&encoded))
	return txn, encoded.Bytes()
}

func TestBuilderSendPrivateBundleSubmitsSignedTransaction(t *testing.T) {
	wantTxn, rawTxn := signedPrivateTransaction(t)
	targetHash := common.Hash{0x33}
	admission := new(recordingPrivateBundleAdmission)
	simulator := &recordingPrivateBundleSimulator{result: &PrivateBundleSimulationResult{Success: true}}
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	t.Cleanup(server.Stop)
	require.NoError(t, server.RegisterName("builder", BuilderAPI(newBuilderAPI(chain.AllProtocolChanges, simulator, admission))))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(client.Close)

	var bundleID common.Hash
	err := client.CallContext(t.Context(), &bundleID, "builder_sendPrivateBundle", PrivateBundleRequest{
		TargetTransaction: targetHash,
		Transaction:       rawTxn,
		TargetSlot:        hexutil.Uint64(42),
	})
	require.NoError(t, err)
	require.Equal(t, wantTxn.Hash(), bundleID)
	require.Equal(t, targetHash, admission.bundle.TargetHash)
	require.Equal(t, uint64(42), admission.bundle.TargetSlot)
	require.Equal(t, wantTxn.Hash(), admission.bundle.Transaction.Hash())
	_, senderFound := admission.bundle.Transaction.GetSender()
	require.True(t, senderFound)
}

func TestBuilderSendPrivateBundleRejectsMalformedTransaction(t *testing.T) {
	api := newBuilderAPI(chain.AllProtocolChanges, &recordingPrivateBundleSimulator{}, new(recordingPrivateBundleAdmission))
	_, err := api.SendPrivateBundle(context.Background(), PrivateBundleRequest{
		TargetTransaction: common.Hash{0x33},
		Transaction:       hexutil.Bytes{0x02, 0x01},
		TargetSlot:        hexutil.Uint64(42),
	})
	require.ErrorContains(t, err, "decode private transaction")
}

func TestBuilderSendPrivateBundleDoesNotSubmitFailedSimulation(t *testing.T) {
	_, rawTxn := signedPrivateTransaction(t)
	admission := new(recordingPrivateBundleAdmission)
	api := newBuilderAPI(chain.AllProtocolChanges, &recordingPrivateBundleSimulator{
		result: &PrivateBundleSimulationResult{Success: false},
	}, admission)

	_, err := api.SendPrivateBundle(t.Context(), PrivateBundleRequest{
		TargetTransaction: common.Hash{0x33},
		Transaction:       rawTxn,
		TargetSlot:        hexutil.Uint64(42),
	})
	require.ErrorContains(t, err, "simulation failed")
	require.Zero(t, admission.calls)
}

func TestBuilderSimulateBundleDoesNotSubmitTransaction(t *testing.T) {
	wantTxn, rawTxn := signedPrivateTransaction(t)
	targetHash := common.Hash{0x33}
	admission := new(recordingPrivateBundleAdmission)
	want := &PrivateBundleSimulationResult{
		Success:            true,
		TargetTransaction:  targetHash,
		PrivateTransaction: wantTxn.Hash(),
		GasUsed:            hexutil.Uint64(41_000),
	}
	simulator := &recordingPrivateBundleSimulator{result: want}
	api := newBuilderAPI(chain.AllProtocolChanges, simulator, admission)

	got, err := api.SimulateBundle(t.Context(), PrivateBundleRequest{
		TargetTransaction: targetHash,
		Transaction:       rawTxn,
		TargetSlot:        hexutil.Uint64(42),
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(t, admission.calls)
	require.Equal(t, targetHash, simulator.targetHash)
	require.Equal(t, wantTxn.Hash(), simulator.txn.Hash())
	_, senderFound := simulator.txn.GetSender()
	require.True(t, senderFound)
}

type recordingPrivateBundleSubmitter struct {
	calls atomic.Uint64
	last  privatepool.Bundle
}

func (s *recordingPrivateBundleSubmitter) Submit(bundle privatepool.Bundle) (common.Hash, error) {
	s.calls.Add(1)
	s.last = bundle
	return bundle.Transaction.Hash(), nil
}

type privateBundleTargetValidatorFunc func(context.Context, common.Hash, uint64, common.Hash, uint64) error

func (fn privateBundleTargetValidatorFunc) ValidateTarget(ctx context.Context, hash common.Hash, slot uint64, parentHash common.Hash, generation uint64) error {
	return fn(ctx, hash, slot, parentHash, generation)
}

func TestActivePrivateBundleAdmissionChecksContextAndTargetAtSubmit(t *testing.T) {
	store := executionbuilder.NewBuildContextStore()
	slot := uint64(42)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	wrapped := store.Wrap(func(context.Context, *executionbuilder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		close(started)
		<-release
		return nil, nil
	})
	go func() {
		defer close(done)
		_, _ = wrapped(t.Context(), &executionbuilder.Parameters{SlotNumber: &slot, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	}()
	<-started
	_, generation, ok := store.Resolve(slot)
	require.True(t, ok)

	submitter := new(recordingPrivateBundleSubmitter)
	var targetValid atomic.Bool
	targetValid.Store(true)
	admission := &activePrivateBundleAdmission{
		submitter: submitter,
		contexts:  store,
		validator: privateBundleTargetValidatorFunc(func(_ context.Context, hash common.Hash, gotSlot uint64, parentHash common.Hash, gotGeneration uint64) error {
			require.Equal(t, common.Hash{0x33}, hash)
			require.Equal(t, slot, gotSlot)
			require.Equal(t, common.Hash{0x44}, parentHash)
			require.Equal(t, generation, gotGeneration)
			if !targetValid.Load() {
				return context.Canceled
			}
			return nil
		}),
	}
	txn, _ := signedPrivateTransaction(t)
	bundle := privatepool.Bundle{TargetHash: common.Hash{0x33}, Transaction: txn, TargetSlot: slot}
	simulation := &PrivateBundleSimulationResult{
		Success:                true,
		TransactionSetRevision: 7,
		ParentBlockHash:        common.Hash{0x44},
		contextGeneration:      generation,
	}
	_, err := admission.Submit(t.Context(), bundle, simulation)
	require.NoError(t, err)
	require.Equal(t, uint64(1), submitter.calls.Load())
	require.Equal(t, simulation.ParentBlockHash, submitter.last.TargetParentHash)
	require.Equal(t, generation, submitter.last.TargetGeneration)

	targetValid.Store(false)
	_, err = admission.Submit(t.Context(), bundle, simulation)
	require.ErrorContains(t, err, "public target changed")
	require.Equal(t, uint64(1), submitter.calls.Load())
	targetValid.Store(true)

	close(release)
	<-done
	nextSlot := slot + 1
	_, err = store.Wrap(func(context.Context, *executionbuilder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, nil
	})(t.Context(), &executionbuilder.Parameters{SlotNumber: &nextSlot, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)
	_, err = admission.Submit(t.Context(), bundle, simulation)
	require.ErrorContains(t, err, "no longer active")
	require.Equal(t, uint64(1), submitter.calls.Load())
}

func TestActivePrivateBundleAdmissionDoesNotHoldStoreLockDuringTargetValidation(t *testing.T) {
	store := executionbuilder.NewBuildContextStore()
	slot := uint64(42)
	buildStarted := make(chan struct{})
	buildRelease := make(chan struct{})
	buildDone := make(chan struct{})
	wrapped := store.Wrap(func(context.Context, *executionbuilder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		close(buildStarted)
		<-buildRelease
		return nil, nil
	})
	go func() {
		defer close(buildDone)
		_, _ = wrapped(t.Context(), &executionbuilder.Parameters{SlotNumber: &slot, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	}()
	<-buildStarted
	_, generation, ok := store.Resolve(slot)
	require.True(t, ok)

	validationStarted := make(chan struct{})
	validationRelease := make(chan struct{})
	admission := &activePrivateBundleAdmission{
		submitter: new(recordingPrivateBundleSubmitter),
		contexts:  store,
		validator: privateBundleTargetValidatorFunc(func(context.Context, common.Hash, uint64, common.Hash, uint64) error {
			close(validationStarted)
			<-validationRelease
			return nil
		}),
	}
	txn, _ := signedPrivateTransaction(t)
	admissionDone := make(chan error, 1)
	go func() {
		_, err := admission.Submit(t.Context(), privatepool.Bundle{TargetHash: common.Hash{0x33}, Transaction: txn, TargetSlot: slot}, &PrivateBundleSimulationResult{
			Success:           true,
			ParentBlockHash:   common.Hash{0x44},
			contextGeneration: generation,
		})
		admissionDone <- err
	}()
	<-validationStarted

	otherSlot := uint64(43)
	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		_, _ = store.Wrap(func(context.Context, *executionbuilder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
			return nil, nil
		})(t.Context(), &executionbuilder.Parameters{SlotNumber: &otherSlot, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	}()
	select {
	case <-otherDone:
	case <-time.After(time.Second):
		t.Fatal("target validation blocked an unrelated build context")
	}

	close(validationRelease)
	require.ErrorContains(t, <-admissionDone, "no longer active")
	close(buildRelease)
	<-buildDone
}
