package downloader

import (
	"context"
	"errors"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
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

func NewGrpcServer(db kv.RwDB, client *Protocols, snapshotDir string, silent bool) (*GrpcServer, error) {
	sn := &GrpcServer{
		db:          db,
		t:           client,
		snapshotDir: snapshotDir,
		silent:      silent,
	}
	return sn, nil
}

func CreateTorrentFilesAndAdd(ctx context.Context, snapshotDir string, torrentClient *torrent.Client) error {
	if err := BuildTorrentFilesIfNeed(ctx, snapshotDir); err != nil {
		return err
	}
	if err := AddTorrentFiles(snapshotDir, torrentClient); err != nil {
		return err
	}
	for _, t := range torrentClient.Torrents() {
		t.AllowDataDownload()
		t.AllowDataUpload()
		t.DownloadAll()
	}
	return nil
}

type GrpcServer struct {
	proto_downloader.UnimplementedDownloaderServer
	t           *Protocols
	db          kv.RwDB
	snapshotDir string
	silent      bool
}

func (s *GrpcServer) Download(ctx context.Context, request *proto_downloader.DownloadRequest) (*emptypb.Empty, error) {
	infoHashes := make([]metainfo.Hash, len(request.Items))
	for i, it := range request.Items {
		//TODO: if hash is empty - create .torrent file from path file (if it exists)
		infoHashes[i] = gointerfaces.ConvertH160toAddress(it.TorrentHash)
	}
	if err := ResolveAbsentTorrents(ctx, s.t.TorrentClient, infoHashes, s.snapshotDir, s.silent); err != nil {
		return nil, err
	}
	for _, t := range s.t.TorrentClient.Torrents() {
		t.AllowDataDownload()
		t.AllowDataUpload()
		t.DownloadAll()
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
