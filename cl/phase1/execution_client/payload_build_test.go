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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

type payloadBuildModuleStub struct {
	execmodule.ExecutionModule
	forkChoice      execmodule.ForkChoiceState
	assembled       execmodule.AssembleBlockResult
	assembleCalls   int
	forkChoiceCalls int
}

func (s *payloadBuildModuleStub) GetForkChoice(context.Context) (execmodule.ForkChoiceState, error) {
	return s.forkChoice, nil
}

func (s *payloadBuildModuleStub) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
	s.forkChoiceCalls++
	return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusSuccess}, nil
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

func TestLocalEngineStartPayloadBuildUsesEnginePayloadIDEncoding(t *testing.T) {
	head := common.Hash{0x41}
	module := &payloadBuildModuleStub{
		forkChoice: execmodule.ForkChoiceState{HeadHash: head},
		assembled:  execmodule.AssembleBlockResult{PayloadID: 42},
	}
	chainRW := chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second)
	client, err := NewExecutionClientEngineLocal(nil, chainRW, nil, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)

	id, err := client.StartPayloadBuild(t.Context(), head, &engine_types.PayloadAttributes{})

	require.NoError(t, err)
	require.Equal(t, []byte(*engine_types.ConvertPayloadId(42)), id)
	require.Equal(t, 1, module.assembleCalls)
	require.Zero(t, module.forkChoiceCalls)
}
