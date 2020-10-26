package snapshotdownloader

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
	"log"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	var opts []grpc.DialOption
	//if *tls {
	//	if *caFile == "" {
	//		*caFile = data.Path("x509/ca_cert.pem")
	//	}
	//	creds, err := credentials.NewClientTLSFromFile(*caFile, *serverHostOverride)
	//	if err != nil {
	//		log.Fatalf("Failed to create TLS credentials %v", err)
	//	}
	//	opts = append(opts, grpc.WithTransportCredentials(creds))
	//} else {
		opts = append(opts, grpc.WithInsecure())
	//}

	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial("127.0.0.1:9191", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	cli:=NewDownloaderClient(conn)
	rep,err:=cli.Download(context.TODO(),&DownloadSnapshotRequest{Networkid: 1, Name: "headers"})
	spew.Dump(rep)
	spew.Dump(err)
	time.Sleep(time.Second)
}