package downloader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ proto_downloader.DownloaderServer = &SNDownloaderServer{}
)

func NewServer(db kv.RwDB, client *Client) (*SNDownloaderServer, error) {
	sn := &SNDownloaderServer{
		db: db,
		t:  client,
	}
	return sn, nil
}

func Stop(torrentClient *torrent.Client) {
	for _, t := range torrentClient.Torrents() {
		t.DisallowDataDownload()
		t.DisallowDataUpload()
	}
}

func Start(ctx context.Context, snapshotDir string, torrentClient *torrent.Client) error {
	if err := BuildTorrentFilesIfNeed(ctx, snapshotDir); err != nil {
		return err
	}
	preverifiedHashes := snapshothashes.Goerli // TODO: remove hard-coded hashes from downloader
	if err := AddTorrentFiles(ctx, snapshotDir, torrentClient, preverifiedHashes); err != nil {
		return err
	}
	for _, t := range torrentClient.Torrents() {
		t.AllowDataDownload()
		t.AllowDataUpload()
	}

	return nil
}

type SNDownloaderServer struct {
	proto_downloader.UnimplementedDownloaderServer
	t  *Client
	db kv.RwDB
}

func (s *SNDownloaderServer) Download(ctx context.Context, request *proto_downloader.DownloadRequest) (*emptypb.Empty, error) {
	infoHashes := make([]metainfo.Hash, len(request.Items))
	for i, it := range request.Items {
		//TODO: if hash is empty - create .torrent file from path file (if it exists)
		infoHashes[i] = gointerfaces.ConvertH160toAddress(it.TorrentHash)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	if err := ResolveAbsentTorrents(ctx, s.t.Cli, infoHashes); err != nil {
		return nil, err
	}
	for _, t := range s.t.Cli.Torrents() {
		t.AllowDataDownload()
		t.AllowDataUpload()
	}
	return &emptypb.Empty{}, nil
}

func (s *SNDownloaderServer) Snapshots(ctx context.Context, request *proto_downloader.SnapshotsRequest) (*proto_downloader.SnapshotsReply, error) {
	torrents := s.t.Cli.Torrents()
	infoItems := make([]*proto_downloader.SnapshotInfo, len(torrents))
	for i, t := range torrents {
		readiness := int32(0)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.GotInfo():
			readiness = int32(100 * (float64(t.BytesCompleted()) / float64(t.Info().TotalLength())))
			if readiness == 100 && !t.Complete.Bool() {
				readiness = 99
			}
		default:
		}

		infoItems[i] = &proto_downloader.SnapshotInfo{
			Readiness: readiness,
			Path:      t.Name(),
		}
	}
	return &proto_downloader.SnapshotsReply{Info: infoItems}, nil
}

func (s *SNDownloaderServer) Stats(ctx context.Context) map[string]torrent.TorrentStats {
	stats := map[string]torrent.TorrentStats{}
	torrents := s.t.Cli.Torrents()
	for _, t := range torrents {
		stats[t.Name()] = t.Stats()
	}
	return stats
}

func GrpcClient(ctx context.Context, downloaderAddr string) (proto_downloader.DownloaderClient, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption

	backoffCfg := backoff.DefaultConfig
	backoffCfg.BaseDelay = 500 * time.Millisecond
	backoffCfg.MaxDelay = 10 * time.Second
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffCfg, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithInsecure())
	conn, err := grpc.DialContext(ctx, downloaderAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return proto_downloader.NewDownloaderClient(conn), nil
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
