package downloader

import (
	"context"
	"fmt"

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

func (s *GrpcServer) Download(ctx context.Context, request *proto_downloader.DownloadRequest) (*emptypb.Empty, error) {
	torrentClient := s.d.Torrent()
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	for _, it := range request.Items {
		if it.TorrentHash == nil {
			err := BuildTorrentAndAdd(ctx, it.Path, s.d.SnapDir(), torrentClient)
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
			if err := CreateTorrentFileIfNotExists(s.d.SnapDir(), t.Info(), &mi); err != nil {
				log.Warn("[downloader] create torrent file", "err", err)
				return
			}
		}(magnet.String())

	}
	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Stats(ctx context.Context, request *proto_downloader.StatsRequest) (*proto_downloader.StatsReply, error) {

	stats := s.d.Stats()
	fmt.Printf("stat call: %t\n", stats.Completed)
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
