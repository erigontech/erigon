package bittorrent

import (
	"context"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"time"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot = errors.New("not supported snapshot for this network id")
)
func NewServer(dir string, seeding bool) *SNDownloaderServer {
	return &SNDownloaderServer{
		t: New(dir,seeding),
		db: ethdb.MustOpen(dir+"/db"),
	}
}

type SNDownloaderServer struct {
	snapshotsync.DownloaderServer
	t *Client
	db ethdb.Database
}

func (S *SNDownloaderServer) Download(ctx context.Context, request *snapshotsync.DownloadSnapshotRequest) (*empty.Empty, error) {
	m,ok:=TorrentHashes[uint64(request.Networkid)]
	if !ok {
		return nil, ErrNotSupportedNetworkID
	}
	var torrents []struct{
		Type snapshotsync.SnapshotType
		Hash metainfo.Hash
	}
	for _,t:=range request.Type {
		hash,ok:=m[t]
		if !ok {
			return nil, ErrNotSupportedSnapshot
		}
		torrents = append(torrents, struct {
			Type snapshotsync.SnapshotType
			Hash metainfo.Hash
		}{Type: t, Hash: hash})
	}
	if len(torrents)==0 {
		return nil, errors.New("empty snapshot types")
	}
	fmt.Println("download")
	for _,t:=range torrents {
		ctx, cancel:=context.WithTimeout(context.Background(), 1 * time.Second)
		defer cancel()
		err :=  S.t.AddTorrent(ctx, S.db, snapshotsync.SnapshotNames[t.Type], t.Hash)
		if err!=nil {
			log.Error("Add torrent failure", "err", err)
			fmt.Println("download-")
			return nil, err
		}
	}
	return &empty.Empty{}, nil
}

func (S *SNDownloaderServer) Snapshots(ctx context.Context, request *empty.Empty) (*snapshotsync.SnapshotsInfoReply, error) {
	reply:= snapshotsync.SnapshotsInfoReply{}
	//for _,t:=range S.t.Cli.Torrents() {
	//	reply.Info=append(reply.Info, &snapshotsync.SnapshotsInfo{
	//		Name: t.Name(),
	//		Networkid: 0,
	//		Downloaded: t.BytesMissing()==0,
	//		Readiness: int32(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),
	//	})
	//}
	//select {
	//case <-ctx.Done():
	//	fmt.Println("Snapshots", ctx.Err())
	//default:
	//	fmt.Println("Default")
	//}
	//
	return &reply, nil
}

