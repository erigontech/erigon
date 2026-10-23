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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	das_state_mock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type identitySentinelClient struct {
	sentinelproto.SentinelClient
}

func (identitySentinelClient) Identity(context.Context, *sentinelproto.EmptyMessage, ...grpc.CallOption) (*sentinelproto.IdentityResponse, error) {
	return &sentinelproto.IdentityResponse{
		Metadata: &sentinelproto.Metadata{Seq: 6, Attnets: "0000000000030000", Syncnets: "0f"},
	}, nil
}

func identityMetadataJSON(t *testing.T, currentEpoch uint64) string {
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 10

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentEpoch().Return(currentEpoch).AnyTimes()
	stateReader := das_state_mock.NewMockPeerDasStateReader(ctrl)
	stateReader.EXPECT().GetAdvertisedCgc().Return(uint64(7)).AnyTimes()
	peerDas := das_mock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().StateReader().Return(stateReader).AnyTimes()

	a := &ApiHandler{sentinel: identitySentinelClient{}, beaconChainCfg: &cfg, ethClock: clock, peerDas: peerDas}
	resp, err := a.GetEthV1NodeIdentity(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/identity", http.NoBody))
	require.NoError(t, err)
	metadata, err := json.Marshal(resp.Data.(map[string]any)["metadata"])
	require.NoError(t, err)
	return string(metadata)
}

func TestNodeIdentityMetadataFollowsSpecFormat(t *testing.T) {
	for _, epoch := range []uint64{10, 11} {
		require.JSONEq(t,
			`{"seq_number":"6","attnets":"0x0000000000030000","syncnets":"0x0f","custody_group_count":"7"}`,
			identityMetadataJSON(t, epoch))
	}
}

func TestNodeIdentityMetadataOmitsCustodyGroupCountBeforeFulu(t *testing.T) {
	require.JSONEq(t,
		`{"seq_number":"6","attnets":"0x0000000000030000","syncnets":"0x0f"}`,
		identityMetadataJSON(t, 9))
}
