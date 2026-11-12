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

package handshake

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

type rawSSZ []byte

func (r rawSSZ) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, r...), nil
}

func (r rawSSZ) EncodingSizeSSZ() int {
	return len(r)
}

func TestValidatePeerDecodesAndCapsErrorMessage(t *testing.T) {
	message := strings.Repeat("x", 300)
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, rawSSZ(message)))

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("REQRESP-RESPONSE-CODE", "2")
		_, _ = w.Write(response.Bytes())
	})
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{}, nil)
	clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).Times(2)
	handshaker := New(t.Context(), clock, &clparams.MainnetBeaconConfig, handler, nil)

	valid, err := handshaker.ValidatePeer(t.Context(), peer.ID("peer"))

	require.False(t, valid)
	require.EqualError(t, err, "hand shake error: 2, "+strings.Repeat("x", 256))
}

func TestValidatePeerReportsCappedHandlerError(t *testing.T) {
	message := strings.Repeat("x", 300)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, message, http.StatusBadRequest)
	})
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{}, nil)
	clock.EXPECT().GetCurrentEpoch().Return(uint64(0)).Times(2)
	handshaker := New(t.Context(), clock, &clparams.MainnetBeaconConfig, handler, nil)

	valid, err := handshaker.ValidatePeer(t.Context(), peer.ID("peer"))

	require.False(t, valid)
	require.EqualError(t, err, "hand shake error: , "+strings.Repeat("x", 256))
	require.NotContains(t, err.Error(), "strconv")
}
