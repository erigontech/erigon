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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestGloasEventTopicsAreValid(t *testing.T) {
	for _, topic := range []beaconevents.EventTopic{
		beaconevents.StateHeadV2,
		beaconevents.OpExecutionPayload,
		beaconevents.OpExecutionPayloadGossip,
		beaconevents.OpExecutionPayloadAvailable,
		beaconevents.OpExecutionPayloadBid,
		beaconevents.OpPayloadAttestationMessage,
		beaconevents.OpProposerPreferences,
	} {
		_, ok := validTopics[topic]
		require.True(t, ok, topic)
	}
}

func TestFastConfirmationTopicIsNotAdvertisedBeforeItHasAProducer(t *testing.T) {
	_, ok := validTopics[beaconevents.EventTopic("fast_confirmation")]
	require.False(t, ok)
}

func TestPayloadAttributesSubscriberTrackedWhileStreamIsOpen(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	emitter := beaconevents.NewEventEmitter()
	handler := &ApiHandler{emitters: emitter, beaconChainCfg: &cfg}
	ctx, cancel := context.WithCancel(t.Context())
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/eth/v1/events?topics=payload_attributes", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.EventSourceGetV1Events(httptest.NewRecorder(), request)
	}()

	require.Eventually(t, emitter.HasPayloadAttributesSubscribers, time.Second, time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.False(t, emitter.HasPayloadAttributesSubscribers())
}
