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

package network

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestAcceptEnvelopeResponsesKeepsOnlyRequestedRoots(t *testing.T) {
	requestedRoot := common.Hash{1}
	unsolicitedRoot := common.Hash{2}
	requestedRoots := map[common.Hash]struct{}{
		requestedRoot: {},
	}
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	requestedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: requestedRoot},
	}

	acceptEnvelopeResponses([]*cltypes.SignedExecutionPayloadEnvelope{
		requestedEnvelope,
		nil,
		{},
		{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: unsolicitedRoot}},
	}, requestedRoots, received)

	require.Same(t, requestedEnvelope, received[requestedRoot])
	require.NotContains(t, received, unsolicitedRoot)
}
