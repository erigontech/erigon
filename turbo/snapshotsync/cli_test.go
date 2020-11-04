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
	//t.Skip()
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial("127.0.0.1:9191", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	cli := NewDownloaderClient(conn)
	//go func() {
	//	for {
	//		fmt.Println("Download start")
	//		rep, err := cli.Download(context.TODO(), &DownloadSnapshotRequest{NetworkId: 1, Type: []SnapshotType{
	//			SnapshotType_headers,
	//			SnapshotType_bodies,
	//			//SnapshotType_State,
	//			//SnapshotType_Receipts,
	//		}})
	//		spew.Dump("Download",rep,err)
	//		time.Sleep(time.Second*20)
	//	}
	//}()
	//

	for {
		reply, err := cli.Snapshots(context.TODO(), &SnapshotsRequest{
			NetworkId: 1,
		})
		spew.Dump("Snapshots",reply,err)


		time.Sleep(time.Second*10)


	}
}
