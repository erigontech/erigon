package downloader

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ proto_downloader.DownloaderServer = &GrpcServer{}
)

func NewGrpcServer(db kv.RwDB, client *Protocols, snapshotDir *dir.Rw, silent bool) (*GrpcServer, error) {
	sn := &GrpcServer{
		db:          db,
		t:           client,
		snapshotDir: snapshotDir,
		silent:      silent,
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
	for _, t := range torrentClient.Torrents() {
		t.AllowDataUpload()
		if !t.Complete.Bool() {
			t.AllowDataDownload()
			t.DownloadAll()
		}
	}
	return nil
}

type GrpcServer struct {
	proto_downloader.UnimplementedDownloaderServer
	t           *Protocols
	db          kv.RwDB
	snapshotDir *dir.Rw
	silent      bool
}

func (s *GrpcServer) Download(ctx context.Context, request *proto_downloader.DownloadRequest) (*emptypb.Empty, error) {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	infoHashes := make([]metainfo.Hash, len(request.Items))
	for i, it := range request.Items {
		if it.TorrentHash == nil {
			if err := BuildTorrentFileIfNeed(ctx, it.Path, s.snapshotDir); err != nil {
				return nil, err
			}
			metaInfo, err := AddTorrentFile(ctx, filepath.Join(s.snapshotDir.Path, it.Path+".torrent"), s.t.TorrentClient)
			if err != nil {
				return nil, err
			}
			infoHashes[i] = metaInfo.HashInfoBytes()
		} else {
			infoHashes[i] = gointerfaces.ConvertH160toAddress(it.TorrentHash)
		}
		if _, ok := s.t.TorrentClient.Torrent(infoHashes[i]); !ok {
			magnet := mi.Magnet(&infoHashes[i], nil)
			if _, err := s.t.TorrentClient.AddMagnet(magnet.String()); err != nil {
				return nil, err
			}
		}
	}
	if len(infoHashes) == 1 {
		t, ok := s.t.TorrentClient.Torrent(infoHashes[0])
		if !ok {
			return nil, fmt.Errorf("torrent not found: [%x]", infoHashes[0])
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.GotInfo():
			metaInfo := t.Metainfo()
			if err := CreateTorrentFileIfNotExists(s.snapshotDir, t.Info(), &metaInfo); err != nil {
				return nil, err
			}
		}
		t.AllowDataUpload()
		t.AllowDataDownload()
		if !t.Complete.Bool() {
			t.DownloadAll()
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *GrpcServer) Stats(ctx context.Context, request *proto_downloader.StatsRequest) (*proto_downloader.StatsReply, error) {
	torrents := s.t.TorrentClient.Torrents()
	reply := &proto_downloader.StatsReply{Completed: true, Torrents: int32(len(torrents))}

	peers := map[torrent.PeerID]struct{}{}

	for _, t := range torrents {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.GotInfo():
			reply.BytesCompleted += uint64(t.BytesCompleted())
			reply.BytesTotal += uint64(t.Info().TotalLength())
			reply.Completed = reply.Completed && t.Complete.Bool()
			reply.Connections += uint64(len(t.PeerConns()))
			for _, peer := range t.PeerConns() {
				peers[peer.PeerID] = struct{}{}
			}
		default:
			reply.Completed = false
		}
	}

	reply.Peers = int32(len(peers))
	reply.Progress = int32(100 * (float64(reply.BytesCompleted) / float64(reply.BytesTotal)))
	if reply.Progress == 100 && !reply.Completed {
		reply.Progress = 99
	}
	return reply, nil
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
