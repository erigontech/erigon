package snapshotsync

import (
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"google.golang.org/grpc"
)

//go:generate ls ./../../interfaces/snapshot_downloader
//go:generate protoc --go_out=. --go-grpc_out=. --proto_path=./../../interfaces/snapshot_downloader "external_downloader.proto" -I=. -I=./../../build/include/google

func NewClient(addr string) (proto_downloader.DownloaderClient, func() error, error) {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, nil, err
	}

	return proto_downloader.NewDownloaderClient(conn), conn.Close, nil
}
