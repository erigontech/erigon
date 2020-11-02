package snapshotsync

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
	"log"
	"testing"
	"time"
)

func TestDownloaderCli(t *testing.T) {
	t.Skip()
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
		rep,err:=cli.Snapshots(context.TODO(), &SnapshotsRequest{
			NetworkId: 1,
		})
		spew.Dump(rep)
		spew.Dump(err)
		time.Sleep(time.Second)
		if i>1 {
			break
		}
		i++
	}

	for {
		rep,err:=cli.Download(context.TODO(),&DownloadSnapshotRequest{NetworkId: 1, Type: []SnapshotType{
			SnapshotType_headers,
			SnapshotType_bodies,
			//SnapshotType_State,
			//SnapshotType_Receipts,
		}})
		spew.Dump(rep)
		spew.Dump(err)
		time.Sleep(time.Second)
		reply,err:=cli.Snapshots(context.TODO(), &SnapshotsRequest{
			NetworkId: 1,
		})
		spew.Dump(reply)
		spew.Dump(err)

	}
}