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

package remotedb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestGetLatestForwardsMaxStep(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := remoteproto.NewMockKVClient(ctrl)
	var request *remoteproto.GetLatestReq
	client.EXPECT().GetLatest(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *remoteproto.GetLatestReq, _ ...grpc.CallOption) (*remoteproto.GetLatestReply, error) {
		request = req
		return &remoteproto.GetLatestReply{}, nil
	})
	tx := &tx{ctx: t.Context(), db: &DB{remoteKV: client}, id: 7}
	_, _, err := tx.GetLatest(kv.AccountsDomain, []byte("key"), kv.GetLatestOptions{}.WithMaxStep(3))
	require.NoError(t, err)
	require.NotNil(t, request.MaxStep)
	require.Equal(t, uint64(3), request.GetMaxStep())
}

func TestGetLatestForwardsBranchCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := remoteproto.NewMockKVClient(ctrl)
	var request *remoteproto.GetLatestReq
	client.EXPECT().GetLatest(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *remoteproto.GetLatestReq, _ ...grpc.CallOption) (*remoteproto.GetLatestReply, error) {
		request = req
		return &remoteproto.GetLatestReply{}, nil
	})
	tx := &tx{ctx: t.Context(), db: &DB{remoteKV: client}, id: 7}
	_, _, err := tx.GetLatest(kv.CommitmentDomain, []byte("key"), kv.GetLatestOptions{}.WithBranchCache())
	require.NoError(t, err)
	require.True(t, request.GetBranchCache())
}
