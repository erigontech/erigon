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
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	prototypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
)

var (
	_ proto_downloader.DownloaderServer = &GrpcServer{}
)

func NewGrpcServer(d *Downloader) (*GrpcServer, error) {
	svr := &GrpcServer{
		d: d,
	}

	return svr, nil
}

type GrpcServer struct {
	proto_downloader.UnimplementedDownloaderServer
	d *Downloader
}

func (s *GrpcServer) ProhibitNewDownloads(ctx context.Context, req *proto_downloader.ProhibitNewDownloadsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// Add files to the downloader. Existing/New files - both ok.
// "download once" invariant: means after initial download finiwh - future restart/upgrade/downgrade will not download files (our "fast restart" feature)
// After "download once": Erigon will produce and seed new files
// Downloader will be able: seed new files (already existing on FS), download uncomplete parts of existing files (if Verify found some bad parts)
func (s *GrpcServer) Add(ctx context.Context, request *proto_downloader.AddRequest) (*emptypb.Empty, error) {
	if len(request.Items) == 0 {
		// Avoid logging initializing 0 torrents.
		return nil, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.d.ResetLogInterval()

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

	var progress atomic.Int32

	go func() {
		logProgress := func() {
			log.Info("[snapshots] initializing downloads", "torrents", fmt.Sprintf("%d/%d", progress.Load(), len(request.Items)))
		}
		defer logProgress()
		interval := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				if interval < time.Minute {
					interval *= 2
				}
			}
			logProgress()
		}
	}()

	for i, it := range request.Items {
		progress.Store(int32(i))
		if it.Path == "" {
			return nil, errors.New("field 'path' is required")
		}

		if it.TorrentHash == nil {
			// if we don't have the torrent hash then we seed a new snapshot
			// TODO: Make the torrent in place then call addPreverifiedTorrent.
			if err := s.d.AddNewSeedableFile(ctx, it.Path); err != nil {
				return nil, err
			}
		} else {
			// There's no circuit breaker in Downloader.RequestSnapshot.
			if ctx.Err() != nil {
				return nil, context.Cause(ctx)
			}
			ih := Proto2InfoHash(it.TorrentHash)
			if err := s.d.RequestSnapshot(ih, it.Path); err != nil {
				err = fmt.Errorf("requesting snapshot %s with infohash %v: %w", it.Path, ih, err)
				return nil, err
			}
		}
	}
	s.d.afterAdd()
	progress.Store(int32(len(request.Items)))

	return &emptypb.Empty{}, nil
}

// Delete - stop seeding, remove file, remove .torrent
func (s *GrpcServer) Delete(ctx context.Context, request *proto_downloader.DeleteRequest) (_ *emptypb.Empty, err error) {
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

func Proto2InfoHash(in *prototypes.H160) metainfo.Hash {
	return gointerfaces.ConvertH160toAddress(in)
}

func (s *GrpcServer) SetLogPrefix(ctx context.Context, request *proto_downloader.SetLogPrefixRequest) (*emptypb.Empty, error) {
	s.d.SetLogPrefix(request.Prefix)

	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Completed(ctx context.Context, request *proto_downloader.CompletedRequest) (*proto_downloader.CompletedReply, error) {
	return &proto_downloader.CompletedReply{Completed: s.d.Completed()}, nil
}
