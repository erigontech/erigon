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

package beaconevents

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestGloasEventFeeds(t *testing.T) {
	emitter := NewEventEmitter()
	stateEvents := make(chan *EventStream, 1)
	stateSubscription := emitter.State().Subscribe(stateEvents)
	defer stateSubscription.Unsubscribe()
	operationEvents := make(chan *EventStream, 3)
	operationSubscription := emitter.Operation().Subscribe(operationEvents)
	defer operationSubscription.Unsubscribe()

	head := &HeadV2Data{Version: "gloas", Data: HeadV2Content{Block: common.Hash{1}, PayloadStatus: "full"}}
	emitter.State().SendHeadV2(head)
	require.Equal(t, &EventStream{Event: StateHeadV2, Data: head}, <-stateEvents)

	payload := &ExecutionPayloadData{BlockRoot: common.Hash{2}}
	emitter.Operation().SendExecutionPayload(payload)
	require.Equal(t, &EventStream{Event: OpExecutionPayload, Data: payload}, <-operationEvents)
	payloadGossip := &ExecutionPayloadGossipData{BlockRoot: common.Hash{2}}
	emitter.Operation().SendExecutionPayloadGossip(payloadGossip)
	require.Equal(t, &EventStream{Event: OpExecutionPayloadGossip, Data: payloadGossip}, <-operationEvents)

	preferences := &VersionedSignedProposerPreferences{Version: "gloas", Data: &cltypes.SignedProposerPreferences{}}
	emitter.Operation().SendProposerPreferences(preferences)
	require.Equal(t, &EventStream{Event: OpProposerPreferences, Data: preferences}, <-operationEvents)
}
