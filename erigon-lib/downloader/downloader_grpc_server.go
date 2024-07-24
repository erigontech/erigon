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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	prototypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
)

var (
	_ proto_downloader.DownloaderServer = &GrpcServer{}
)

func NewGrpcServer(d *Downloader) (*GrpcServer, error) {
	return &GrpcServer{d: d}, nil
}

type GrpcServer struct {
	proto_downloader.UnimplementedDownloaderServer
	d *Downloader
}

func (s *GrpcServer) ProhibitNewDownloads(ctx context.Context, req *proto_downloader.ProhibitNewDownloadsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.d.torrentFS.ProhibitNewDownloads(req.Type)
}

// Erigon "download once" - means restart/upgrade/downgrade will not download files (and will be fast)
// After "download once" - Erigon will produce and seed new files
// Downloader will able: seed new files (already existing on FS), download uncomplete parts of existing files (if Verify found some bad parts)
func (s *GrpcServer) Add(ctx context.Context, request *proto_downloader.AddRequest) (*emptypb.Empty, error) {
	defer s.d.ReCalcStats(10 * time.Second) // immediately call ReCalc to set stat.Complete flag

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	for i, it := range request.Items {
		if it.Path == "" {
			return nil, fmt.Errorf("field 'path' is required")
		}

		select {
		case <-logEvery.C:
			log.Info("[snapshots] initializing", "files", fmt.Sprintf("%d/%d", i, len(request.Items)))
		default:
		}

		if it.TorrentHash == nil {
			// if we don't have the torrent hash then we seed a new snapshot
			if err := s.d.AddNewSeedableFile(ctx, it.Path); err != nil {
				return nil, err
			}
			continue
		}

		if err := s.d.AddMagnetLink(ctx, Proto2InfoHash(it.TorrentHash), it.Path); err != nil {
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

// Delete - stop seeding, remove file, remove .torrent
func (s *GrpcServer) Delete(ctx context.Context, request *proto_downloader.DeleteRequest) (*emptypb.Empty, error) {
	defer s.d.ReCalcStats(10 * time.Second) // immediately call ReCalc to set stat.Complete flag
	torrents := s.d.torrentClient.Torrents()
	for _, name := range request.Paths {
		if name == "" {
			return nil, fmt.Errorf("field 'path' is required")
		}
		for _, t := range torrents {
			select {
			case <-t.GotInfo():
				continue
			default:
			}
			if t.Name() == name {
				t.Drop()
				break
			}
		}

		fPath := filepath.Join(s.d.SnapDir(), name)
		_ = os.Remove(fPath)
		s.d.torrentFS.Delete(name)
	}
	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Verify(ctx context.Context, request *proto_downloader.VerifyRequest) (*emptypb.Empty, error) {
	err := s.d.VerifyData(ctx, nil, false)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Stats(ctx context.Context, request *proto_downloader.StatsRequest) (*proto_downloader.StatsReply, error) {
	stats := s.d.Stats()
	return &proto_downloader.StatsReply{
		MetadataReady: stats.MetadataReady,
		FilesTotal:    stats.FilesTotal,

		Completed: stats.Completed,
		Progress:  stats.Progress,

		PeersUnique:      stats.PeersUnique,
		ConnectionsTotal: stats.ConnectionsTotal,

		BytesCompleted: stats.BytesCompleted,
		BytesTotal:     stats.BytesTotal,
		UploadRate:     stats.UploadRate,
		DownloadRate:   stats.DownloadRate,
	}, nil
}

func Proto2InfoHash(in *prototypes.H160) metainfo.Hash {
	return gointerfaces.ConvertH160toAddress(in)
}
