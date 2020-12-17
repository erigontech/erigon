package bittorrent

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes/empty"
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
	downloader, err := New(dir, seeding)
	if err != nil {
		return nil, err
	}
	return &SNDownloaderServer{
		t:  downloader,
		db: ethdb.MustOpen(dir + "/db"),
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
