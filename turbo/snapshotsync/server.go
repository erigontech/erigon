package snapshotsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/snapshothashes"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ snapshotsync.DownloaderServer = &SNDownloaderServer{}
)

func NewServer(dir string, seeding bool) (*SNDownloaderServer, error) {
	db := mdbx.MustOpen(dir + "/db")
	sn := &SNDownloaderServer{
		db: db,
	}
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		peerID, err := tx.GetOne(kv.BittorrentInfo, []byte(kv.BittorrentPeerID))
		if err != nil {
			return fmt.Errorf("get peer id: %w", err)
		}
		sn.t, err = New(dir, seeding, string(peerID))
		if err != nil {
			return err
		}
		if len(peerID) == 0 {
			err = sn.t.SavePeerID(tx)
			if err != nil {
				return fmt.Errorf("save peer id: %w", err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return sn, nil
}

type SNDownloaderServer struct {
	snapshotsync.UnimplementedDownloaderServer
	t  *Client
	db kv.RwDB
}

func (s *SNDownloaderServer) Download(ctx context.Context, request *snapshotsync.DownloadRequest) (*emptypb.Empty, error) {
	infoHashes := Proto2InfoHashes(request.TorrentHashes)

	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	if err := ResolveAbsentTorrents(ctx, s.t.Cli, infoHashes); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *SNDownloaderServer) Load(ctx context.Context) error {
	if err := BuildTorrentFilesIfNeed(ctx, s.t.snapshotsDir); err != nil {
		return err
	}
	preverifiedHashes := snapshothashes.Goerli // TODO: remove hard-coded hashes from downloader
	if err := AddTorrentFiles(ctx, s.t.snapshotsDir, s.t.Cli, preverifiedHashes); err != nil {
		return err
	}
	return nil
}

func (s *SNDownloaderServer) Snapshots(ctx context.Context, request *snapshotsync.SnapshotsRequest) (*snapshotsync.SnapshotsInfoReply, error) {
	reply := snapshotsync.SnapshotsInfoReply{}
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resp, err := s.t.GetSnapshots(tx)
	if err != nil {
		return nil, err
	}
	for i := range resp {
		reply.Info = append(reply.Info, resp[i])
	}
	return &reply, nil
}

func (s *SNDownloaderServer) Stats(ctx context.Context) map[string]torrent.TorrentStats {
	stats := map[string]torrent.TorrentStats{}
	torrents := s.t.Cli.Torrents()
	for _, t := range torrents {
		stats[t.Name()] = t.Stats()
	}
	return stats
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

func InfoHashes2Proto(in []metainfo.Hash) []*prototypes.H160 {
	infoHashes := make([]*prototypes.H160, len(in))
	i := 0
	for _, h := range in {
		infoHashes[i] = gointerfaces.ConvertAddressToH160(h)
		i++
	}
	return infoHashes
}
