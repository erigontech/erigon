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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type txReplyStream struct {
	remoteproto.KV_TxClient
	replies []*remoteproto.Pair
}

func (s *txReplyStream) Recv() (*remoteproto.Pair, error) {
	if len(s.replies) == 0 {
		return nil, io.EOF
	}
	reply := s.replies[0]
	s.replies = s.replies[1:]
	return reply, nil
}

func (*txReplyStream) Send(*remoteproto.Cursor) error { return nil }
func (*txReplyStream) CloseSend() error               { return nil }

func TestHistoryFilesGenerationMetadata(t *testing.T) {
	for _, tc := range []struct {
		name       string
		generation *uint64
	}{
		{"legacy_server", nil},
		{"zero_generation", new(uint64(0))},
		{"identified_view", new(uint64(42))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := remoteproto.NewMockKVClient(gomock.NewController(t))
			client.EXPECT().Tx(gomock.Any()).Return(&txReplyStream{replies: []*remoteproto.Pair{
				{TxId: 7, ViewId: 5, HistoryFilesGeneration: tc.generation},
			}}, nil)
			db, err := NewRemote(gointerfaces.Version{}, log.New(), client).Open()
			require.NoError(t, err)
			t.Cleanup(db.Close)
			tx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.Equal(t, uint64(5), tx.ViewID())
			files, ok := tx.Debug().(interface{ HistoryFilesGeneration() uint64 })
			require.Equal(t, tc.generation != nil, ok)
			if ok {
				require.Equal(t, *tc.generation, files.HistoryFilesGeneration())
			}
		})
	}
}

func TestCursorReplyUpdatesTransactionView(t *testing.T) {
	for _, tc := range []struct {
		name              string
		initialGeneration *uint64
		replyViewID       uint64
		replyGeneration   *uint64
		wantViewID        uint64
	}{
		{"identified_view", new(uint64(42)), 6, new(uint64(43)), 6},
		{"view_id_only", nil, 6, nil, 6},
		{"legacy_server", nil, 0, nil, 5},
		{"generation_removed", new(uint64(42)), 6, nil, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := remoteproto.NewMockKVClient(gomock.NewController(t))
			client.EXPECT().Tx(gomock.Any()).Return(&txReplyStream{replies: []*remoteproto.Pair{
				{TxId: 7, ViewId: 5, HistoryFilesGeneration: tc.initialGeneration},
				{CursorId: 1, ViewId: tc.replyViewID, HistoryFilesGeneration: tc.replyGeneration},
			}}, nil)
			db, err := NewRemote(gointerfaces.Version{}, log.New(), client).Open()
			require.NoError(t, err)
			t.Cleanup(db.Close)
			tx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			cursor, err := tx.Cursor(kv.MaxTxNum)
			require.NoError(t, err)
			defer cursor.Close()
			require.Equal(t, tc.wantViewID, tx.ViewID())
			files, ok := tx.Debug().(interface{ HistoryFilesGeneration() uint64 })
			require.Equal(t, tc.replyGeneration != nil, ok)
			if ok {
				require.Equal(t, *tc.replyGeneration, files.HistoryFilesGeneration())
			}
		})
	}
}

func TestMaxPrunableStepsBacklog(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := remoteproto.NewMockKVClient(ctrl)
	client.EXPECT().MaxPrunableStepsBacklog(gomock.Any(), gomock.Any(), gomock.Any()).Return(&remoteproto.MaxPrunableStepsBacklogReply{Steps: 123}, nil)
	db := &DB{remoteKV: client}
	require.Equal(t, uint64(123), db.MaxPrunableStepsBacklog())
}

func TestHistoryStartFromPreservesFloor(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := remoteproto.NewMockKVClient(ctrl)
	client.EXPECT().HistoryStartFrom(t.Context(), &remoteproto.HistoryStartFromReq{
		TxId: 7, Domain: uint32(kv.StorageDomain),
	}).Return(&remoteproto.HistoryStartFromReply{StartFrom: 123}, nil)
	tx := &tx{ctx: t.Context(), db: &DB{remoteKV: client}, id: 7}

	start, err := tx.HistoryStartFrom(kv.StorageDomain)
	require.NoError(t, err)
	require.Equal(t, uint64(123), start)
}

func TestHistoryStartFromPropagatesTransportError(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := remoteproto.NewMockKVClient(ctrl)
	wantErr := status.Error(codes.Unavailable, "history unavailable")
	client.EXPECT().HistoryStartFrom(gomock.Any(), gomock.Any()).Return(nil, wantErr)
	tx := &tx{ctx: t.Context(), db: &DB{remoteKV: client}, id: 7}

	start, err := tx.HistoryStartFrom(kv.StorageDomain)
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, start)
}

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
