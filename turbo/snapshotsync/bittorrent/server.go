package bittorrent

import (
	"context"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ snapshotsync.DownloaderServer = &SNDownloaderServer{}
)

func NewServer(dir string, seeding bool) (*SNDownloaderServer, error) {
	db:=ethdb.MustOpen(dir + "/db")
	peerID,err:=db.Get(dbutils.BittorrentInfoBucket, []byte(dbutils.BittorrentPeerID))
	if err!=nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, fmt.Errorf("get peer id: %w",err)
	}
	downloader, err := New(dir, seeding, string(peerID))
	if err != nil {
		return nil, err
	}
	if len(peerID)==0 {
		err = downloader.SavePeerID(db)
		if err!=nil {
			return nil, fmt.Errorf("save peer id: %w",err)
		}
	}
	return &SNDownloaderServer{
		t:  downloader,
		db: db,
	}, nil
}

type SNDownloaderServer struct {
	snapshotsync.DownloaderServer
	t  *Client
	db ethdb.Database
}

func (S *SNDownloaderServer) Download(ctx context.Context, request *snapshotsync.DownloadSnapshotRequest) (*empty.Empty, error) {
	err := S.t.AddSnapshotsTorrents(ctx, S.db, request.NetworkId, snapshotsync.FromSnapshotTypes(request.Type))
	if err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}
func (S *SNDownloaderServer) Load() error {
	return S.t.Load(S.db)
}

func (S *SNDownloaderServer) Snapshots(ctx context.Context, request *snapshotsync.SnapshotsRequest) (*snapshotsync.SnapshotsInfoReply, error) {
	reply := snapshotsync.SnapshotsInfoReply{}
	resp, err := S.t.GetSnapshots(S.db, request.NetworkId)
	if err != nil {
		return nil, err
	}
	for i := range resp {
		reply.Info = append(reply.Info, resp[i])
	}
	return &reply, nil
}

func (S *SNDownloaderServer) Stats(ctx context.Context) (map[string]torrent.TorrentStats) {
	stats:=map[string]torrent.TorrentStats{}
	torrents:=S.t.Cli.Torrents()
	for _,t:=range torrents {
		stats[t.Name()] = t.Stats()
	}
	return stats
}
