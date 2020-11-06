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
	"path/filepath"
)

var (
	ErrNotSupportedNetworkID = errors.New("not supported network id")
	ErrNotSupportedSnapshot  = errors.New("not supported snapshot for this network id")
)
var (
	_ snapshotsync.DownloaderServer = &SNDownloaderServer{}
)

func NewServer(dir string, seeding bool) (*SNDownloaderServer, error) {
	downloader,err:=New(dir, seeding)
	if err!=nil {
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
	err := S.WalkThroughTorrents(request.NetworkId, func(k, v []byte) (bool, error) {
		var hash metainfo.Hash
		if len(v) != metainfo.HashSize {
			return true, nil
		}
		copy(hash[:], v)
		t, ok := S.t.Cli.Torrent(hash)
		if !ok {
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

		_, tpStr := ParseInfoHashKey(k)
		tp, ok := snapshotsync.SnapshotType_value[tpStr]
		if !ok {
			return false, fmt.Errorf("incorrect type: %v", tpStr)
		}

		val := &snapshotsync.SnapshotsInfo{
			Type:          snapshotsync.SnapshotType(tp),
			GotInfoByte:   gotInfo,
			Readiness:     readiness,
			SnapshotBlock: SnapshotBlock,
			Dbpath:        filepath.Join(S.t.snapshotsDir, t.Files()[0].Path()),
		}
		reply.Info = append(reply.Info, val)

		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func (S *SNDownloaderServer) WalkThroughTorrents(networkID uint64, f func(k, v []byte) (bool, error)) error {
	networkIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(networkIDBytes, networkID)
	return S.db.Walk(dbutils.SnapshotInfoBucket, append(networkIDBytes, []byte(SnapshotInfoHashPrefix)...), 8*8+16, f)
}
