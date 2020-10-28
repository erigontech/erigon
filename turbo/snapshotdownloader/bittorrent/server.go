package bittorrent

import (
	"context"
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotdownloader"
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
	snapshotdownloader.DownloaderServer
	t *Client
	db ethdb.Database
}

func (S *SNDownloaderServer) Download(ctx context.Context, request *snapshotdownloader.DownloadSnapshotRequest) (*empty.Empty, error) {
	m,ok:=TorrentHashes[uint64(request.Networkid)]
	if !ok {
		return nil, ErrNotSupportedNetworkID
	}
	hash,ok:=m[request.Name]
	if !ok {
		return nil, ErrNotSupportedSnapshot
	}
	ctx, cancel:=context.WithTimeout(ctx, 1 * time.Second)
	defer cancel()
	err :=  S.t.AddTorrent(ctx, S.db, request.Name, hash)
	if err!=nil {
		log.Error("Add torrent failure", "err", err)
		return nil, err
	}
	//var readiness int32
	//if t, ok:=S.t.Cli.Torrent(hash); ok && t!=nil {
	//	readiness = int32(100*(float64(t.BytesCompleted())/float64(t.BytesMissing()+t.BytesCompleted())))
	//}
	return &empty.Empty{}, nil
}

func (S *SNDownloaderServer) Snapshots(ctx context.Context, request *empty.Empty) (*snapshotdownloader.SnapshotsInfoReply, error) {
	reply:= snapshotdownloader.SnapshotsInfoReply{}
	//for _,t:=range S.t.Cli.Torrents() {
	//	reply.Info=append(reply.Info, &SnapshotsInfoReply_SnapshotsInfo{
	//		Name: t.Name(),
	//		Networkid: 0,
	//		Downloaded: t.BytesMissing()==0,
	//		Readiness: int32(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),
	//	})
	//}
	fmt.Println("---------")
	spew.Dump(request)
	return &reply, nil
}

