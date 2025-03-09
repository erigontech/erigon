// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
)

type StateChangesClientMock struct {
	ctx   context.Context
	recvC <-chan *remoteproto.StateChangeBatch
}

func NewStateChangesClientMock(ctx context.Context, recvC <-chan *remoteproto.StateChangeBatch) *StateChangesClientMock {
	return &StateChangesClientMock{ctx: ctx, recvC: recvC}
}

func (s StateChangesClientMock) StateChanges(context.Context, *remoteproto.StateChangeRequest, ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
	return kvStateChangesStreamMock{ctx: s.ctx, recvC: s.recvC}, nil
}

var _ remoteproto.KV_StateChangesClient = kvStateChangesStreamMock{}

type kvStateChangesStreamMock struct {
	ctx   context.Context
	recvC <-chan *remoteproto.StateChangeBatch
}

func (k kvStateChangesStreamMock) Recv() (*remoteproto.StateChangeBatch, error) {
	for {
		select {
		case <-k.ctx.Done():
			return nil, k.ctx.Err()
		case batch := <-k.recvC:
			return batch, nil
		}
	}
}

func (k kvStateChangesStreamMock) Header() (metadata.MD, error) {
	panic("kvStateChangesStreamMock.Header not supported")
}

func (k kvStateChangesStreamMock) Trailer() metadata.MD {
	panic("kvStateChangesStreamMock.Trailer not supported")
}

func (k kvStateChangesStreamMock) CloseSend() error {
	panic("kvStateChangesStreamMock.CloseSend not supported")
}

func (k kvStateChangesStreamMock) Context() context.Context {
	panic("kvStateChangesStreamMock.Context not supported")
}

func (k kvStateChangesStreamMock) SendMsg(any) error {
	panic("kvStateChangesStreamMock.SendMsg not supported")
}

func (k kvStateChangesStreamMock) RecvMsg(any) error {
	panic("kvStateChangesStreamMock.RecvMsg not supported")
}
