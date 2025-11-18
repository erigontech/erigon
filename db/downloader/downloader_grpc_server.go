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

type GrpcServer struct {
	downloaderproto.UnimplementedDownloaderServer
	d *Downloader
}

// Add files to the downloader. Existing/New files - both ok.
// "download once" invariant: means after initial download finish - future restart/upgrade/downgrade will not download files (our "fast restart" feature)
// After "download once": Erigon will produce and seed new files
// Downloader will be able: seed new files (already existing on FS), download uncomplete parts of existing files (if Verify found some bad parts)
func (s *GrpcServer) Add(ctx context.Context, request *downloaderproto.AddRequest) (*emptypb.Empty, error) {
	{
		var names []string
		for _, name := range request.Items {
			if filepath.IsAbs(name.Path) {
				return nil, fmt.Errorf("assert: Downloader.GrpcServer.Add called with absolute path %s, please use filepath.Rel(dirs.Snap, filePath)", name.Path)
			}
			names = append(names, name.Path)
		}
		s.d.logger.Debug("[snapshots] Downloader.Add", "files", names)
	}

	var preverifiedSnapshots []PreverifiedSnapshot

	for _, it := range request.Items {
		if it.Path == "" {
			return nil, errors.New("field 'path' is required")
		}
		if it.TorrentHash == nil {
			// if we don't have the torrent hash then we seed a new snapshot
			if err := s.d.AddNewSeedableFile(ctx, it.Path); err != nil {
				return nil, err
			}
		} else {
			// TODO: Handle this case as a separate call to avoid mistakes.
			ih := Proto2InfoHash(it.TorrentHash)
			preverifiedSnapshots = append(preverifiedSnapshots, PreverifiedSnapshot{
				InfoHash: ih,
				Name:     it.Path,
			})
		}
	}

	return &emptypb.Empty{}, s.d.DownloadSnapshots(ctx, preverifiedSnapshots, request.LogTarget)
}

// Delete - stop seeding, remove file, remove .torrent
func (s *GrpcServer) Delete(ctx context.Context, request *downloaderproto.DeleteRequest) (_ *emptypb.Empty, err error) {
	{
		var names []string
		for _, relPath := range request.Paths {
			if filepath.IsAbs(relPath) {
				return nil, fmt.Errorf("assert: Downloader.GrpcServer.Add called with absolute path %s, please use filepath.Rel(dirs.Snap, filePath)", relPath)
			}
			names = append(names, relPath)
		}
		s.d.logger.Debug("[snapshots] Downloader.Delete", "files", names)
	}

	for _, name := range request.Paths {
		if name == "" {
			err = errors.Join(err, errors.New("field 'path' is required"))
			continue
		}
		err = errors.Join(err, s.d.Delete(name))
	}
	if err == nil {
		return &emptypb.Empty{}, nil
	}
	return
}

func Proto2InfoHash(in *typesproto.H160) metainfo.Hash {
	return gointerfaces.ConvertH160toAddress(in)
}

func (s *GrpcServer) SetLogPrefix(ctx context.Context, request *downloaderproto.SetLogPrefixRequest) (*emptypb.Empty, error) {
	s.d.SetLogPrefix(request.Prefix)

	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Completed(ctx context.Context, request *downloaderproto.CompletedRequest) (*downloaderproto.CompletedReply, error) {
	return &downloaderproto.CompletedReply{Completed: s.d.Completed()}, nil
}
