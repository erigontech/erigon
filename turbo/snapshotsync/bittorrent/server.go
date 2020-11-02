package bittorrent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
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

func NewServer(dir string, seeding bool) *SNDownloaderServer {
	return &SNDownloaderServer{
		t:  New(dir, seeding),
		db: ethdb.MustOpen(dir + "/db"),
	}
}

type SNDownloaderServer struct {
	snapshotsync.DownloaderServer
	t  *Client
	db ethdb.Database
}

func (S *SNDownloaderServer) Download(ctx context.Context, request *snapshotsync.DownloadSnapshotRequest) (*empty.Empty, error) {
	m, ok := TorrentHashes[request.NetworkId]
	if !ok {
		return nil, ErrNotSupportedNetworkID
	}

	for _, t := range request.Type {
		hash, ok := m[t]
		if !ok {
			return nil, ErrNotSupportedSnapshot
		}
		err := S.t.AddTorrentSpec(context.Background(), S.db, t.String(), hash, nil)
		if err != nil {
			return nil, err
		}
	}
	return &empty.Empty{}, nil
}

func (S *SNDownloaderServer) Snapshots(ctx context.Context, request *snapshotsync.SnapshotsRequest) (*snapshotsync.SnapshotsInfoReply, error) {
	reply := snapshotsync.SnapshotsInfoReply{}
	err := S.WalkThroughTorrents(request.NetworkId, func(k, v []byte) (bool, error) {
		var hash metainfo.Hash
		if len(v) != metainfo.HashSize {
			fmt.Println("incorrect length", len(v))
			return true, nil
		}
		copy(hash[:], v)
		t, ok := S.t.Cli.Torrent(hash)
		if !ok {
			fmt.Println("No torrent for hash", string(k), " h= ", hash.String())
			return true, nil
		}

		var gotInfo bool
		readiness := int32(0)
		select {
		case <-t.GotInfo():
			gotInfo = true
			readiness = int32(100 * (float64(t.BytesCompleted()) / float64(t.Info().TotalLength())))

		default:
		}

		tp := snapshotsync.SnapshotType_value[string(k[8+2:])]

		val := &snapshotsync.SnapshotsInfo{
			Type:          snapshotsync.SnapshotType(tp),
			GotInfoByte:   gotInfo,
			Readiness:     readiness,
			SnapshotBlock: SnapshotBlock,
		}
		reply.Info = append(reply.Info, val)

		return true, nil
	})
	if err != nil {
		return nil, err
	}
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

	return &reply, nil
}

func (S *SNDownloaderServer) WalkThroughTorrents(networkID uint64, f func(k, v []byte) (bool, error)) error {
	networkIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(networkIDBytes, networkID)
	return S.db.Walk(dbutils.SnapshotInfoBucket, append(networkIDBytes, []byte(SnapshotInfoHashPrefix)...), 8*8+16, f)
}
