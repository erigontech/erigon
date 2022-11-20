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
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
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

// Download - create new .torrent ONLY if initialSync, everything else Erigon can generate by itself
func (s *GrpcServer) Download(ctx context.Context, request *proto_downloader.DownloadRequest) (*emptypb.Empty, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	torrentClient := s.d.Torrent()
	snapDir := s.d.SnapDir()
	for i, it := range request.Items {
		select {
		case <-logEvery.C:
			log.Info("[snapshots] initializing", "files", fmt.Sprintf("%d/%d", i, len(request.Items)))
		default:
		}

		if it.TorrentHash == nil {
			// if we dont have the torrent hash then we seed a new snapshot
			log.Info("[snapshots] seeding a new snapshot")
			ok, err := seedNewSnapshot(it, torrentClient, snapDir)
			if err != nil {
				return nil, err
			}
			if ok {
				log.Debug("[snapshots] already have both seg and torrent file")
			} else {
				log.Warn("[snapshots] didn't get the seg or the torrent file")
			}
			continue
		}

		_, err := createMagnetLinkWithInfoHash(it.TorrentHash, torrentClient, snapDir)
		if err != nil {
			return nil, err
		}
	}
	s.d.ReCalcStats(10 * time.Second) // immediately call ReCalc to set stat.Complete flag
	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Verify(ctx context.Context, request *proto_downloader.VerifyRequest) (*emptypb.Empty, error) {
	err := s.d.verify()
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

// decides what we do depending on wether we have the .seg file or the .torrent file
// have .torrent no .seg => get .seg file from .torrent
// have .seg no .torrent => get .torrent from .seg
func seedNewSnapshot(it *proto_downloader.DownloadItem, torrentClient *torrent.Client, snapDir string) (bool, error) {
	// if we dont have the torrent file we build it if we have the .seg file
	if err := buildTorrentIfNeed(it.Path, snapDir); err != nil {
		return false, err
	}

	// we add the .seg file we have and create the .torrent file if we dont have it
	ok, err := AddSegment(it.Path, snapDir, torrentClient)
	if err != nil {
		return false, fmt.Errorf("AddSegment: %w", err)
	}

	// torrent file does exist and seg
	if !ok {
		return false, nil
	}

	// we skip the item in for loop since we build the seg and torrent file here
	return true, nil
}

// we dont have .seg or .torrent so we get them through the torrent hash
func createMagnetLinkWithInfoHash(hash *prototypes.H160, torrentClient *torrent.Client, snapDir string) (bool, error) {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	if hash == nil {
		return false, nil
	}
	infoHash := Proto2InfoHash(hash)
	//log.Debug("[downloader] downloading torrent and seg file", "hash", infoHash)

	if _, ok := torrentClient.Torrent(infoHash); ok {
		//log.Debug("[downloader] torrent client related to hash found", "hash", infoHash)
		return true, nil
	}

	magnet := mi.Magnet(&infoHash, nil)
	go func(magnetUrl string) {
		t, err := torrentClient.AddMagnet(magnetUrl)
		if err != nil {
			log.Warn("[downloader] add magnet link", "err", err)
			return
		}
		t.DisallowDataDownload()
		t.AllowDataUpload()
		<-t.GotInfo()

		mi := t.Metainfo()
		if err := CreateTorrentFileIfNotExists(snapDir, t.Info(), &mi); err != nil {
			log.Warn("[downloader] create torrent file", "err", err)
			return
		}
	}(magnet.String())
	//log.Debug("[downloader] downloaded both seg and torrent files", "hash", infoHash)
	return false, nil
}
