package downloader

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/anacrolix/torrent"
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

func CreateTorrentFilesAndAdd(ctx context.Context, snapshotDir *dir.Rw, torrentClient *torrent.Client) error {
	if err := BuildTorrentFilesIfNeed(ctx, snapshotDir); err != nil {
		return err
	}
	if err := AddTorrentFiles(ctx, snapshotDir, torrentClient); err != nil {
		return err
	}
	return nil
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
	preverifiedHashes := make([]metainfo.Hash, len(request.Items))
	for i, it := range request.Items {
		if it.TorrentHash == nil {
			if err := BuildTorrentFileIfNeed(ctx, it.Path, s.snapshotDir); err != nil {
				return nil, err
			}
			metaInfo, err := AddTorrentFile(ctx, filepath.Join(s.snapshotDir.Path, it.Path+".torrent"), s.t.TorrentClient)
			if err != nil {
				return nil, err
			}
			preverifiedHashes[i] = metaInfo.HashInfoBytes()
		} else {
			preverifiedHashes[i] = gointerfaces.ConvertH160toAddress(it.TorrentHash)
		}

		if _, ok := torrentClient.Torrent(preverifiedHashes[i]); ok {
			continue
		}
		magnet := mi.Magnet(&preverifiedHashes[i], nil)
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

func Proto2InfoHashes(in []*prototypes.H160) []metainfo.Hash {
	infoHashes := make([]metainfo.Hash, len(in))
	i := 0
	for _, h := range in {
		infoHashes[i] = gointerfaces.ConvertH160toAddress(h)
		i++
	}
	return infoHashes
}
