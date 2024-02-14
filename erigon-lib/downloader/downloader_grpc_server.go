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

package downloader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
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

func (s *GrpcServer) ProhibitNewDownloads(context.Context, *proto_downloader.ProhibitNewDownloadsRequest) (*emptypb.Empty, error) {
	if err := s.d.torrentFiles.prohibitNewDownloads(); err != nil {
		return nil, err
	}
	return nil, nil
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
		s.d.torrentFiles.Delete(name)
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
