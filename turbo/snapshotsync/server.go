package snapshotsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/anacrolix/torrent"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
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

func (s *SNDownloaderServer) Download(ctx context.Context, request *snapshotsync.DownloadSnapshotRequest) (*emptypb.Empty, error) {
	if err := s.db.Update(ctx, func(tx kv.RwTx) error {
		return s.t.AddSnapshotsTorrents(ctx, tx, request.NetworkId, FromSnapshotTypes(request.Type))
	}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
func (s *SNDownloaderServer) Load() error {
	return s.db.View(context.Background(), func(tx kv.Tx) error {
		return s.t.Load(tx)
	})
}

func (s *SNDownloaderServer) Snapshots(ctx context.Context, request *snapshotsync.SnapshotsRequest) (*snapshotsync.SnapshotsInfoReply, error) {
	reply := snapshotsync.SnapshotsInfoReply{}
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resp, err := s.t.GetSnapshots(tx, request.NetworkId)
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
