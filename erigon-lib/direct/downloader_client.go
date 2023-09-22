/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package direct

import (
	"context"

	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DownloaderClient struct {
	server proto_downloader.DownloaderServer
}

func NewDownloaderClient(server proto_downloader.DownloaderServer) *DownloaderClient {
	return &DownloaderClient{server: server}
}

func (c *DownloaderClient) Download(ctx context.Context, in *proto_downloader.DownloadRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Download(ctx, in)
}
func (c *DownloaderClient) Verify(ctx context.Context, in *proto_downloader.VerifyRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Verify(ctx, in)
}
func (c *DownloaderClient) Stats(ctx context.Context, in *proto_downloader.StatsRequest, opts ...grpc.CallOption) (*proto_downloader.StatsReply, error) {
	return c.server.Stats(ctx, in)
}
