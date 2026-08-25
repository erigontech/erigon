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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestBuildHeadV2DataUsesGenesisRootInEpochZeroAndOne(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	genesisRoot := common.Hash{1}
	epochZeroEndRoot := common.Hash{2}
	headRoot := common.Hash{3}
	stateRoot := common.Hash{4}

	tests := []struct {
		name        string
		slot        uint64
		currentRoot common.Hash
		nextRoot    common.Hash
	}{
		{name: "epoch_zero", slot: 1, currentRoot: genesisRoot, nextRoot: genesisRoot},
		{name: "epoch_one", slot: cfg.SlotsPerEpoch + 1, currentRoot: genesisRoot, nextRoot: epochZeroEndRoot},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			headState := state.New(&cfg)
			headState.SetVersion(clparams.GloasVersion)
			headState.SetSlot(test.slot)
			headState.SetBlockRootAt(0, genesisRoot)
			headState.SetBlockRootAt(int(cfg.SlotsPerEpoch-1), epochZeroEndRoot)

			event, err := BuildHeadV2Data(&cfg, headState, test.slot, headRoot, stateRoot, "full", true)
			require.NoError(t, err)
			require.Equal(t, test.currentRoot, event.Data.CurrentEpochDependentRoot)
			require.Equal(t, test.nextRoot, event.Data.NextEpochDependentRoot)
			require.Equal(t, stateRoot, event.Data.State)
			require.Equal(t, "full", event.Data.PayloadStatus)
			require.True(t, event.Data.ExecutionOptimistic)
		})
	}
}

func TestBuildHeadV2DataDoesNotReadPrunedGenesisRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 2
	cfg.SlotsPerHistoricalRoot = 8
	headState := state.New(&cfg)
	headState.SetVersion(clparams.GloasVersion)
	headState.SetSlot(10)
	currentRoot := common.Hash{1}
	nextRoot := common.Hash{2}
	headState.SetBlockRootAt(7, currentRoot)
	headState.SetBlockRootAt(1, nextRoot)

	event, err := BuildHeadV2Data(&cfg, headState, 10, common.Hash{3}, common.Hash{4}, "full", false)
	require.NoError(t, err)
	require.Equal(t, currentRoot, event.Data.CurrentEpochDependentRoot)
	require.Equal(t, nextRoot, event.Data.NextEpochDependentRoot)
}

func TestGloasEventFeedsDoNotBlockOnSlowSubscriber(t *testing.T) {
	tests := []struct {
		name      string
		subscribe func(*EventEmitter, chan *EventStream) func()
		send      func(*EventEmitter) int
	}{
		{
			name: "payload_attestation_message",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.Operation().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int {
				return emitter.Operation().SendPayloadAttestationMessage(&PayloadAttestationMessageData{})
			},
		},
		{
			name: "execution_payload_bid",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.Operation().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int {
				return emitter.Operation().SendExecutionPayloadBid(&SignedExecutionPayloadBidData{})
			},
		},
		{
			name: "execution_payload_available",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.Operation().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int {
				return emitter.Operation().SendExecutionPayloadAvailable(&ExecutionPayloadAvailableData{})
			},
		},
		{
			name: "execution_payload",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.Operation().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int {
				return emitter.Operation().SendExecutionPayload(&ExecutionPayloadData{})
			},
		},
		{
			name: "execution_payload_gossip",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.Operation().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int {
				return emitter.Operation().SendExecutionPayloadGossip(&ExecutionPayloadGossipData{})
			},
		},
		{
			name: "proposer_preferences",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.Operation().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int {
				return emitter.Operation().SendProposerPreferences(&VersionedSignedProposerPreferences{})
			},
		},
		{
			name: "head_v2",
			subscribe: func(emitter *EventEmitter, ch chan *EventStream) func() {
				sub := emitter.State().Subscribe(ch)
				return sub.Unsubscribe
			},
			send: func(emitter *EventEmitter) int { return emitter.State().SendHeadV2(&HeadV2Data{}) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			emitter := NewEventEmitter()
			unsubscribe := test.subscribe(emitter, make(chan *EventStream))
			defer unsubscribe()
			completed := make(chan int, 1)
			go func() { completed <- test.send(emitter) }()
			select {
			case delivered := <-completed:
				require.Zero(t, delivered)
			case <-time.After(time.Second):
				t.Fatal("Gloas event emission blocked on a slow subscriber")
			}
		})
	}
}

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
