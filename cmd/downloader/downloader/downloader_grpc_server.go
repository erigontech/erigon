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
		var ok bool
		var err error
		if it.TorrentHash == nil {
			// if we dont have the torrent hash then we seed a new snapshot
			ok, err = seedNewSnapshot(it, torrentClient, snapDir)
			if err != nil {
				return nil, err
			}
		}

		select {
		case <-logEvery.C:
			log.Info("[snapshots] initializing", "files", fmt.Sprintf("%d/%d", i, len(request.Items)))
		default:
		}
		if ok {
			continue
		}

		ok, err = createMagnetLinkWithInfoHash(it.TorrentHash, torrentClient, snapDir)
		if err != nil {
			return nil, err
		}

		if ok {
			continue
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

func seedNewSnapshot(it *proto_downloader.DownloadItem, torrentClient *torrent.Client, snapDir string) (bool, error) {
	// if we dont have the torrent file we build it
	if err := BuildTorrentFileIfNeed(it.Path, snapDir); err != nil {
		return false, err
	}

	ok, err := AddSegment(it.Path, snapDir, torrentClient)
	if err != nil {
		return false, fmt.Errorf("AddSegment: %w", err)
	}

	if !ok {
		return createMagnetLinkWithInfoHash(nil, torrentClient, snapDir)
	}

	return true, nil
}

// we dont have .seg or .torrent so we get them through the torrent hash
func createMagnetLinkWithInfoHash(hash *prototypes.H160, torrentClient *torrent.Client, snapDir string) (bool, error) {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	var infoHash *metainfo.Hash
	if hash != nil {
		*infoHash = Proto2InfoHash(hash)
		if _, ok := torrentClient.Torrent(*infoHash); ok {
			return true, nil
		}
	}
	magnet := mi.Magnet(infoHash, nil)
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

	return false, nil
}
