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

package privateapi

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestNewEthBackendServerRejectsNilChainConfig(t *testing.T) {
	require.PanicsWithValue(t, "privateapi: NewEthBackendServer: nil chainConfig", func() {
		NewEthBackendServer(t.Context(), nil, nil, nil, nil, log.New(), nil, nil)
	})
}

func TestBlockBodyRejectsInvalidHash(t *testing.T) {
	server := &EthBackendServer{}
	for _, tc := range []struct {
		name string
		hash *typesproto.H256
	}{
		{"missing", nil},
		{"empty", &typesproto.H256{}},
		{"missing high half", &typesproto.H256{Lo: &typesproto.H128{}}},
		{"missing low half", &typesproto.H256{Hi: &typesproto.H128{}}},
		{"zero", &typesproto.H256{Hi: &typesproto.H128{}, Lo: &typesproto.H128{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.BlockBody(t.Context(), &remoteproto.BlockRequest{BlockHash: tc.hash, BlockHeight: 1})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestClientVersionIncludesCommit(t *testing.T) {
	origCommit := version.GitCommit
	t.Cleanup(func() { version.GitCommit = origCommit })
	version.GitCommit = "a53e954520442aa91a92f111eb23b213ffc800b7"

	reply, err := (&EthBackendServer{}).ClientVersion(t.Context(), &remoteproto.ClientVersionRequest{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(reply.NodeName, "erigon/"+version.VersionWithMeta+"-a53e9545/"), reply.NodeName)
}
