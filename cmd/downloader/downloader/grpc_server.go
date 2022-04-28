package downloader

import (
	"context"
	"errors"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ proto_downloader.DownloaderServer = &GrpcServer{}
)

func NewGrpcServer(db kv.RwDB, client *Protocols, snapshotDir *dir.Rw) (*GrpcServer, error) {
	sn := &GrpcServer{
		db:          db,
		t:           client,
		snapshotDir: snapshotDir,
	}
	return sn, nil
}

type GrpcServer struct {
	proto_downloader.UnimplementedDownloaderServer
	t           *Protocols
	db          kv.RwDB
	snapshotDir *dir.Rw
}

func (s *GrpcServer) Download(ctx context.Context, request *proto_downloader.DownloadRequest) (*emptypb.Empty, error) {
	torrentClient := s.t.TorrentClient
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	for _, it := range request.Items {
		if it.TorrentHash == nil {
			_, err := BuildTorrentAndAdd(ctx, it.Path, s.snapshotDir, s.t.TorrentClient)
			if err != nil {
				return nil, err
			}
			continue
		}

		hash := Proto2InfoHash(it.TorrentHash)
		if _, ok := torrentClient.Torrent(hash); ok {
			continue
		}

		magnet := mi.Magnet(&hash, nil)
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
			if err := CreateTorrentFileIfNotExists(s.snapshotDir, t.Info(), &mi); err != nil {
				log.Warn("[downloader] create torrent file", "err", err)
				return
			}
		}(magnet.String())

	}
	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Stats(ctx context.Context, request *proto_downloader.StatsRequest) (*proto_downloader.StatsReply, error) {
	stats := s.t.Stats()
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
