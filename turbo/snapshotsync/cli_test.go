package snapshotsync

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"testing"
	"time"
)

func TestDownloaderCli(t *testing.T) {
	opts:=[]grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial("127.0.0.1:9191", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	cli:=NewDownloaderClient(conn)
	i:=0
	for {
		rep,err:=cli.Snapshots(context.TODO(), &empty.Empty{})
		spew.Dump(rep)
		spew.Dump(err)
		time.Sleep(time.Second)
		if i>1 {
			break
		}
		i++
	}

	for {
		rep,err:=cli.Download(context.TODO(),&DownloadSnapshotRequest{Networkid: 1, Type: []SnapshotType{
			SnapshotType_Headers,
			SnapshotType_Bodies,
			//SnapshotType_State,
			//SnapshotType_Receipts,
		}})
		spew.Dump(rep)
		spew.Dump(err)
		time.Sleep(time.Second)
		reply,err:=cli.Snapshots(context.TODO(), &empty.Empty{})
		spew.Dump(reply)
		spew.Dump(err)

	}
}