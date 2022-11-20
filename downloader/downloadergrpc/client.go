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

package downloadergrpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func NewClient(ctx context.Context, downloaderAddr string) (proto_downloader.DownloaderClient, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption

	backoffCfg := backoff.DefaultConfig
	backoffCfg.BaseDelay = 500 * time.Millisecond
	backoffCfg.MaxDelay = 10 * time.Second
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffCfg, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.DialContext(ctx, downloaderAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return proto_downloader.NewDownloaderClient(conn), nil
}

func InfoHashes2Proto(in []metainfo.Hash) []*prototypes.H160 {
	infoHashes := make([]*prototypes.H160, len(in))
	i := 0
	for _, h := range in {
		infoHashes[i] = gointerfaces.ConvertAddressToH160(h)
		i++
	}
	return infoHashes
}

func Strings2Proto(in []string) []*prototypes.H160 {
	infoHashes := make([]*prototypes.H160, len(in))
	i := 0
	for _, h := range in {
		infoHashes[i] = String2Proto(h)
		i++
	}
	return infoHashes
}

func String2Proto(in string) *prototypes.H160 {
	var infoHash [20]byte
	inHex, _ := hex.DecodeString(in)
	copy(infoHash[:], inHex)
	return gointerfaces.ConvertAddressToH160(infoHash)
}
