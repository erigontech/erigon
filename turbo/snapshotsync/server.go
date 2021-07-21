package snapshotsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/anacrolix/torrent"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ DownloaderServer = &SNDownloaderServer{}
)

func NewServer(dir string, seeding bool) (*SNDownloaderServer, error) {
	db := kv.MustOpen(dir + "/db")
	sn := &SNDownloaderServer{
		db: kv.NewObjectDatabase(db),
	}
	if err := db.Update(context.Background(), func(tx ethdb.RwTx) error {
		peerID, err := tx.GetOne(dbutils.BittorrentInfoBucket, []byte(dbutils.BittorrentPeerID))
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
	DownloaderServer
	t  *Client
	db ethdb.Database
}

func (s *SNDownloaderServer) Download(ctx context.Context, request *DownloadSnapshotRequest) (*empty.Empty, error) {
	err := s.t.AddSnapshotsTorrents(ctx, s.db, request.NetworkId, FromSnapshotTypes(request.Type))
	if err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}
func (s *SNDownloaderServer) Load() error {
	return s.t.Load(s.db)
}

func (s *SNDownloaderServer) Snapshots(ctx context.Context, request *SnapshotsRequest) (*SnapshotsInfoReply, error) {
	reply := SnapshotsInfoReply{}
	resp, err := s.t.GetSnapshots(s.db, request.NetworkId)
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
