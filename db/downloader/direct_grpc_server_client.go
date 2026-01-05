// Copyright 2021 The Erigon Authors
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

package downloader

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// DirectGrpcServerClient wraps a downloaderproto.DownloaderServer to implement
// the downloaderproto.DownloaderClient interface for direct in-process calls.
type DirectGrpcServerClient struct {
	server downloaderproto.DownloaderServer
}

func NewDirectGrpcServerClient(server downloaderproto.DownloaderServer) *DirectGrpcServerClient {
	return &DirectGrpcServerClient{server: server}
}

func (c *DirectGrpcServerClient) Download(ctx context.Context, in *downloaderproto.DownloadRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Download(ctx, in)
}

func (c *DirectGrpcServerClient) Seed(ctx context.Context, in *downloaderproto.SeedRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Seed(ctx, in)
}

func (c *DirectGrpcServerClient) Delete(ctx context.Context, in *downloaderproto.DeleteRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Delete(ctx, in)
}
