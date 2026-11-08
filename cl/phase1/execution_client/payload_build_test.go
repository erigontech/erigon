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

package execution_client

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

type payloadBuildModuleStub struct {
	execmodule.ExecutionModule
	forkChoice       execmodule.ForkChoiceState
	forkChoiceResult execmodule.ForkChoiceResult
	assembled        execmodule.AssembleBlockResult
	assembleCalls    int
	forkChoiceCalls  int
}

func (s *payloadBuildModuleStub) GetForkChoice(context.Context) (execmodule.ForkChoiceState, error) {
	return s.forkChoice, nil
}

func (s *payloadBuildModuleStub) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
	s.forkChoiceCalls++
	return s.forkChoiceResult, nil
}

func (s *payloadBuildModuleStub) AssembleBlock(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	s.assembleCalls++
	return s.assembled, nil
}

func TestDirectStartPayloadBuildDoesNotUpdateForkChoice(t *testing.T) {
	head := common.Hash{0x41}
	module := &payloadBuildModuleStub{
		forkChoice: execmodule.ForkChoiceState{HeadHash: head},
		assembled:  execmodule.AssembleBlockResult{PayloadID: 42},
	}
	client, err := NewExecutionClientDirect(
		chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil,
	)
	require.NoError(t, err)

	id, err := client.StartPayloadBuild(t.Context(), head, &engine_types.PayloadAttributes{})

	require.NoError(t, err)
	want := make([]byte, 8)
	binary.LittleEndian.PutUint64(want, 42)
	require.Equal(t, want, id)
	require.Equal(t, 1, module.assembleCalls)
	require.Zero(t, module.forkChoiceCalls)
}

func TestDirectForkChoiceUpdateReportsBusyWhenUpdateWasNotStarted(t *testing.T) {
	module := &payloadBuildModuleStub{
		forkChoiceResult: execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusBusy},
	}
	client, err := NewExecutionClientDirect(
		chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil,
	)
	require.NoError(t, err)

	_, err = client.ForkChoiceUpdate(t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.ElectraVersion)

	require.ErrorIs(t, err, ErrForkChoiceUpdateBusy)
}

func TestDirectForkChoiceUpdateAllowsBusyUpdateInProgress(t *testing.T) {
	module := &payloadBuildModuleStub{
		forkChoiceResult: execmodule.ForkChoiceResult{
			Status:           execmodule.ExecutionStatusBusy,
			UpdateInProgress: true,
		},
	}
	client, err := NewExecutionClientDirect(
		chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil,
	)
	require.NoError(t, err)

	id, err := client.ForkChoiceUpdate(t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.ElectraVersion)

	require.NoError(t, err)
	require.Nil(t, id)
}

func TestDirectForkChoiceUpdateKeepsBuildingAfterBusyWithAttributes(t *testing.T) {
	module := &payloadBuildModuleStub{
		forkChoiceResult: execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusBusy},
		assembled:        execmodule.AssembleBlockResult{PayloadID: 42},
	}
	client, err := NewExecutionClientDirect(
		chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil,
	)
	require.NoError(t, err)

	id, err := client.ForkChoiceUpdate(
		t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, &engine_types.PayloadAttributes{}, clparams.ElectraVersion,
	)

	require.NoError(t, err)
	require.Equal(t, encodeDirectPayloadID(42), id)
	require.Equal(t, 1, module.assembleCalls)
}

func TestStartPayloadBuildRequiresMatchingExecutionHead(t *testing.T) {
	module := &payloadBuildModuleStub{
		forkChoice: execmodule.ForkChoiceState{HeadHash: common.Hash{0x41}},
		assembled:  execmodule.AssembleBlockResult{PayloadID: 42},
	}
	client, err := NewExecutionClientDirect(
		chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil,
	)
	require.NoError(t, err)

	_, err = client.StartPayloadBuild(t.Context(), common.Hash{0x42}, &engine_types.PayloadAttributes{})

	require.ErrorIs(t, err, ErrPayloadBuildHeadMismatch)
	require.Zero(t, module.assembleCalls)
}

func TestEngineClientDoesNotExposeDirectPayloadBuild(t *testing.T) {
	_, ok := any(&ExecutionClientEngine{}).(PayloadBuilder)
	require.False(t, ok)
}

func TestDirectPreparationAndForkChoiceReuseTheSameExecutionBuild(t *testing.T) {
	module := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := module.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, module.InsertChain(chainPack))

	chainRW := chainreader.NewChainReaderEth1(module.ChainConfig, module.ExecModule, time.Hour)
	client, err := NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)
	head := chainPack.TopBlock.Hash()
	// Parallel state flushing can outlive a successful fork-choice response. Wait for the setup work
	// so this test isolates build reuse from expected execution-module contention.
	module.ExecModule.WaitIdle(t.Context())
	_, err = client.ForkChoiceUpdate(t.Context(), head, head, head, nil, clparams.ElectraVersion)
	require.NoError(t, err)
	module.ExecModule.WaitIdle(t.Context())

	timestamp := hexutil.Uint64(time.Now().Add(12 * time.Second).Unix())
	parentRoot := common.Hash{0x41}
	attributes := &engine_types.PayloadAttributes{
		Timestamp:             timestamp,
		PrevRandao:            common.Hash{0x42},
		SuggestedFeeRecipient: common.Address{0x43},
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &parentRoot,
	}
	preparedID, err := client.StartPayloadBuild(t.Context(), head, attributes)
	require.NoError(t, err)
	productionID, err := client.ForkChoiceUpdate(
		t.Context(), head, head, head, attributes, clparams.ElectraVersion,
	)
	require.NoError(t, err)

	require.NotEmpty(t, preparedID)
	require.Equal(t, preparedID, productionID)
}

func TestDirectSkipsValidationBelowTracksTheHead(t *testing.T) {
	newClient := func(blocks int) *ExecutionClientDirect {
		module := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
		if blocks > 0 {
			chainPack, err := module.GenerateChain(blocks, nil)
			require.NoError(t, err)
			require.NoError(t, module.InsertChain(chainPack))
		}
		client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(module.ChainConfig, module.ExecModule, time.Hour), nil)
		require.NoError(t, err)
		return client
	}

	below, known := newClient(0).SkipsValidationBelow(t.Context())
	require.True(t, known)
	require.Zero(t, below, "a head at the first blocks validates every block")
	below, known = newClient(6).SkipsValidationBelow(t.Context())
	require.True(t, known)
	require.Equal(t, uint64(6-reorgTooDeepDepth), below)

	ended, cancel := context.WithCancel(t.Context())
	cancel()
	_, known = newClient(6).SkipsValidationBelow(ended)
	require.False(t, known, "an unreadable head is not a depth of zero")
}

// insertThenCancelModule stores nothing and ends the caller's context once the insert returns, as
// a client that gives up while the payload is being inserted does.
type insertThenCancelModule struct {
	execmodule.ExecutionModule
	cancel context.CancelFunc
}

func (m insertThenCancelModule) InsertBlocks(context.Context, []*types.Block) (execmodule.ExecutionStatus, error) {
	m.cancel()
	return execmodule.ExecutionStatusSuccess, nil
}

// A head read that fails because the caller's context ended is no reason to answer NotValidated.
func TestDirectNewPayloadReportsAnEndedContextAsNoVerdict(t *testing.T) {
	module := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	ctx, cancel := context.WithCancel(t.Context())
	chainRW := chainreader.NewChainReaderEth1(module.ChainConfig, insertThenCancelModule{ExecutionModule: module.ExecModule, cancel: cancel}, time.Hour)
	client, err := NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)

	payload := gloas(&clparams.MainnetBeaconConfig, nil)
	parentRoot := common.Hash{}
	payload.BlockHash, err = payload.ComputeBlockHash(&parentRoot, cltypes.ComputeExecutionRequestHash(nil), nil)
	require.NoError(t, err)

	status, err := client.NewPayload(ctx, payload, &parentRoot, nil, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, PayloadStatusNone, status)
	require.True(t, ValidationInterrupted(ctx, status, err))
}
