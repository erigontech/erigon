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

package direct

import (
	"context"
	"io"

	proto_downloader "github.com/erigontech/erigon/erigon-lib/gointerfaces/downloaderproto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DownloaderClient struct {
	server proto_downloader.DownloaderServer
}

func NewDownloaderClient(server proto_downloader.DownloaderServer) *DownloaderClient {
	return &DownloaderClient{server: server}
}

func (c *DownloaderClient) Add(ctx context.Context, in *proto_downloader.AddRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Add(ctx, in)
}

func (c *DownloaderClient) ProhibitNewDownloads(ctx context.Context, in *proto_downloader.ProhibitNewDownloadsRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.ProhibitNewDownloads(ctx, in)
}
func (c *DownloaderClient) Delete(ctx context.Context, in *proto_downloader.DeleteRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Delete(ctx, in)
}
func (c *DownloaderClient) Verify(ctx context.Context, in *proto_downloader.VerifyRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Verify(ctx, in)
}
func (c *DownloaderClient) SetLogPrefix(ctx context.Context, in *proto_downloader.SetLogPrefixRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.SetLogPrefix(ctx, in)
}
func (c *DownloaderClient) Completed(ctx context.Context, in *proto_downloader.CompletedRequest, opts ...grpc.CallOption) (*proto_downloader.CompletedReply, error) {
	return c.server.Completed(ctx, in)
}

func (c *DownloaderClient) TorrentCompleted(ctx context.Context, in *proto_downloader.TorrentCompletedRequest, opts ...grpc.CallOption) (proto_downloader.Downloader_TorrentCompletedClient, error) {
	ch := make(chan *downloadedReply, 1<<16)
	streamServer := &DownloadeSubscribeS{ch: ch, ctx: ctx}

	go func() {
		streamServer.Err(c.server.TorrentCompleted(in, streamServer))
	}()

	return &DownloadeSubscribeC{ch: ch, ctx: ctx}, nil
}

type DownloadeSubscribeC struct {
	ch  chan *downloadedReply
	ctx context.Context
	grpc.ClientStream
}

func (c *DownloadeSubscribeC) Recv() (*proto_downloader.TorrentCompletedReply, error) {
	if c.ctx.Err() != nil {
		return nil, io.EOF
	}

	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *DownloadeSubscribeC) Context() context.Context { return c.ctx }

type DownloadeSubscribeS struct {
	ch  chan *downloadedReply
	ctx context.Context
	grpc.ServerStream
}

type downloadedReply struct {
	r   *proto_downloader.TorrentCompletedReply
	err error
}

func (s *DownloadeSubscribeS) Send(m *proto_downloader.TorrentCompletedReply) error {
	if s.ctx.Err() != nil {
		if s.ch != nil {
			ch := s.ch
			s.ch = nil
			close(ch)
		}
		return s.ctx.Err()
	}

	s.ch <- &downloadedReply{r: m}
	return nil
}
func (s *DownloadeSubscribeS) Context() context.Context { return s.ctx }
func (s *DownloadeSubscribeS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &downloadedReply{err: err}
}
