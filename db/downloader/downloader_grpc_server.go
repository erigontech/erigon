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
	"errors"
	"fmt"
	"path/filepath"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon/common/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

var (
	_ downloaderproto.DownloaderServer = &GrpcServer{}
)

func NewGrpcServer(d *Downloader) (*GrpcServer, error) {
	svr := &GrpcServer{
		d: d,
	}

	return svr, nil
}

// Error relating to using a snap name for RPC. Such as it is absolute or non-local to the
// client-side.
type errRpcSnapName struct {
	error
}

type GrpcServer struct {
	downloaderproto.UnimplementedDownloaderServer
	d *Downloader
}

func (s *GrpcServer) checkNamesAndLogCall(names []string, callName string) error {
	for _, name := range names {
		if name == "" {
			return errors.New("field 'path' is required")
		}
		if filepath.IsAbs(name) {
			return fmt.Errorf("assert: Downloader.GrpcServer called with absolute path %q, please use filepath.Rel(dirs.Snap, filePath) before RPC", name)
		}
	}
	s.d.log(log.LvlDebug, fmt.Sprintf("Downloader.%s", callName), "files", names)
	return nil
}

// "download once" invariant: means after initial download finish - future restart/upgrade/downgrade will not download files (our "fast restart" feature)
// After "download once": Erigon will produce and seed new files
func (s *GrpcServer) Download(ctx context.Context, request *downloaderproto.DownloadRequest) (_ *emptypb.Empty, err error) {
	preverifiedSnapshots := make([]preverifiedSnapshot, 0, len(request.Items))
	names := make([]string, 0, len(request.Items))
	for _, it := range request.Items {
		if it.TorrentHash == nil {
			err = fmt.Errorf("request for %q missing required torrent hash", it.Path)
			return
		}
		ih := Proto2InfoHash(it.TorrentHash)
		preverifiedSnapshots = append(preverifiedSnapshots, preverifiedSnapshot{
			InfoHash: ih,
			Name:     it.Path,
		})
		names = append(names, it.Path)
	}
	err = s.checkNamesAndLogCall(names, "Download")
	if err != nil {
		return
	}
	return &emptypb.Empty{}, s.d.DownloadSnapshots(ctx, preverifiedSnapshots, request.LogTarget)
}

// Add existing files to the downloader.
// Erigon will produce and seed new files
// Downloader will be able: seed new files (already existing on FS).
func (s *GrpcServer) Seed(ctx context.Context, request *downloaderproto.SeedRequest) (_ *emptypb.Empty, err error) {
	names := request.Paths
	err = s.checkNamesAndLogCall(names, "Seed")
	if err != nil {
		return
	}
	for _, name := range names {
		err = s.d.AddNewSeedableFile(ctx, name)
		if err != nil {
			err = fmt.Errorf("adding %q: %w", name, err)
			return
		}
	}
	return
}

// Delete - stop seeding, remove file, remove .torrent
func (s *GrpcServer) Delete(ctx context.Context, request *downloaderproto.DeleteRequest) (_ *emptypb.Empty, err error) {
	err = s.checkNamesAndLogCall(request.Paths, "Delete")
	for _, name := range request.Paths {
		err = errors.Join(err, s.d.Delete(name))
	}
	return
}

func Proto2InfoHash(in *typesproto.H160) metainfo.Hash {
	return gointerfaces.ConvertH160toAddress(in)
}
