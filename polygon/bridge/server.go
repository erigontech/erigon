// Copyright 2024 The Erigon Authors
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

package bridge

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/execution/types"
)

type bridgeReader interface {
	Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash common.Hash) (uint64, bool, error)
}

var APIVersion = &typesproto.VersionReply{Major: 1, Minor: 0, Patch: 0}

type BackendServer struct {
	remoteproto.UnimplementedBridgeBackendServer // must be embedded to have forward compatible implementations.

	ctx          context.Context
	bridgeReader bridgeReader
}

func NewBackendServer(ctx context.Context, bridgeReader bridgeReader) *BackendServer {
	return &BackendServer{
		ctx:          ctx,
		bridgeReader: bridgeReader,
	}
}

func (b *BackendServer) Version(ctx context.Context, in *emptypb.Empty) (*typesproto.VersionReply, error) {
	return APIVersion, nil
}

func (b *BackendServer) BorTxnLookup(ctx context.Context, in *remoteproto.BorTxnLookupRequest) (*remoteproto.BorTxnLookupReply, error) {
	blockNum, ok, err := b.bridgeReader.EventTxnLookup(ctx, gointerfaces.ConvertH256ToHash(in.BorTxHash))
	if err != nil {
		return nil, err
	}

	return &remoteproto.BorTxnLookupReply{
		Present:     ok,
		BlockNumber: blockNum,
	}, nil
}

func (b *BackendServer) BorEvents(ctx context.Context, in *remoteproto.BorEventsRequest) (*remoteproto.BorEventsReply, error) {
	events, err := b.bridgeReader.Events(ctx, gointerfaces.ConvertH256ToHash(in.BlockHash), in.BlockNum)
	if err != nil {
		return nil, err
	}

	eventsRaw := make([][]byte, len(events))
	for i, event := range events {
		eventsRaw[i] = event.Data()
	}

	return &remoteproto.BorEventsReply{
		EventRlps: eventsRaw,
	}, nil
}
