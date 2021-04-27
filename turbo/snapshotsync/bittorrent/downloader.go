package bittorrent

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"path/filepath"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	Cli          *torrent.Client
	snapshotsDir string
}

func New(snapshotsDir string, seeding bool, peerID string) (*Client, error) {
	torrentConfig := DefaultTorrentConfig()
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"
	torrentConfig.PeerID=peerID

	torrentClient, err := torrent.NewClient(torrentConfig)
	if err != nil {
		log.Error("Fail to start torrnet client", "err", err)
		return nil, fmt.Errorf("fail to start: %w",err)
	}

	return &Client{
		Cli:          torrentClient,
		snapshotsDir: snapshotsDir,
	}, nil
}

func DefaultTorrentConfig() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort = 0
	torrentConfig.NoDHT = true
	torrentConfig.DisableTrackers = false
	torrentConfig.Debug = false
	torrentConfig.Logger = torrentConfig.Logger.FilterLevel(lg.Debug)
	torrentConfig.Logger = NewAdapterLogger()
	return torrentConfig
}

func (cli *Client) Torrents() []metainfo.Hash {
	t:=cli.Cli.Torrents()
	hashes:=make([]metainfo.Hash, 0, len(t))
	for k,v:=range t {
		fmt.Println(k, v.Name(), v.InfoHash().String())
		hashes = append(hashes,v.InfoHash())
	}
	return hashes
}
func (cli *Client) Load(db ethdb.Database) error {
	log.Info("Load added torrents")
	return db.Walk(dbutils.SnapshotInfoBucket, []byte{}, 0, func(k, infoHashBytes []byte) (bool, error) {
		if !bytes.HasPrefix(k[8:], []byte(SnapshotInfoHashPrefix)) {
			return true, nil
		}
		networkID, snapshotName := ParseInfoHashKey(k)
		infoHash := metainfo.Hash{}
		copy(infoHash[:], infoHashBytes)
		infoBytes, err := db.GetOne(dbutils.SnapshotInfoBucket, MakeInfoBytesKey(snapshotName, networkID))
		if err != nil {
			return false, err
		}

		log.Info("Add torrent", "snapshot", snapshotName, "hash", infoHash.String(), "infobytes", len(infoBytes) > 0)
		_, err = cli.AddTorrentSpec(snapshotName, infoHash, infoBytes)
		if err != nil {
			return false, err
		}

		return true, nil
	})
}

func (cli *Client) SavePeerID(db ethdb.Putter) error {
	return db.Put(dbutils.BittorrentInfoBucket, []byte(dbutils.BittorrentPeerID), cli.PeerID())
}

func (cli *Client) Close() {
	cli.Cli.Close()
}


func (cli *Client) PeerID() []byte {
	peerID:=cli.Cli.PeerID()
	return peerID[:]
}
func (cli *Client) AddTorrentSpec(snapshotName string, snapshotHash metainfo.Hash, infoBytes []byte) (*torrent.Torrent, error) {
	t, ok := cli.Cli.Torrent(snapshotHash)
	if ok {
		return t, nil
	}
	t, _, err := cli.Cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:    Trackers,
		InfoHash:    snapshotHash,
		DisplayName: snapshotName,
		InfoBytes:   infoBytes,
	})
	return t, err
}

func (cli *Client) AddTorrent(ctx context.Context, db ethdb.Database, snapshotType snapshotsync.SnapshotType, networkID uint64) error { //nolint: interfacer
	infoHashBytes, infoBytes, err := getTorrentSpec(db, snapshotType.String(), networkID)
	if err != nil {
		return err
	}
	var infoHash metainfo.Hash
	newTorrent := false
	if infoHashBytes != nil {
		copy(infoHash[:], infoHashBytes[:metainfo.HashSize])
	} else {
		log.Info("Init new torrent", "snapshot", snapshotType.String())
		newTorrent = true
		var ok bool
		infoHash, ok = TorrentHashes[networkID][snapshotType]
		if !ok {
			return fmt.Errorf("%w type %v, networkID %v", ErrInvalidSnapshot, snapshotType, networkID)
		}
	}
	log.Info("Added torrent spec", "snapshot", snapshotType.String(), "hash", infoHash.String())
	t, err := cli.AddTorrentSpec(snapshotType.String(), infoHash, infoBytes)
	if err != nil {
		return fmt.Errorf("error on add snapshot: %w", err)
	}
	log.Info("Getting infobytes", "snapshot", snapshotType.String())
	infoBytes, err = cli.GetInfoBytes(context.Background(), infoHash)
	if err != nil {
		log.Warn("Init failure", "snapshot", snapshotType.String(), "err", ctx.Err())
		return fmt.Errorf("error on get info bytes: %w", err)
	}
	t.AllowDataDownload()
	t.DownloadAll()
	log.Info("Got infobytes", "snapshot", snapshotType.String(), "file", t.Files()[0].Path())

	if newTorrent {
		log.Info("Save spec", "snapshot", snapshotType.String())
		err := saveTorrentSpec(db, snapshotType.String(), networkID, infoHash, infoBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cli *Client) GetInfoBytes(ctx context.Context, snapshotHash metainfo.Hash) ([]byte, error) {
	t, ok := cli.Cli.Torrent(snapshotHash)
	if !ok {
		return nil, errors.New("torrent not added")
	}
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("add torrent timeout: %w", ctx.Err())
		case <-t.GotInfo():
			return common.CopyBytes(t.Metainfo().InfoBytes), nil
		default:
			log.Info("Searching infobytes", "seeders", t.Stats().ConnectedSeeders, "active peers", t.Stats().ActivePeers)
			time.Sleep(time.Second * 60)
		}
	}
}

func (cli *Client) AddSnapshotsTorrents(ctx context.Context, db ethdb.Database, networkId uint64, mode snapshotsync.SnapshotMode) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	eg := errgroup.Group{}

	if mode.Headers {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_headers, networkId)
		})
	}

	if mode.Bodies {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_bodies, networkId)
		})
	}

	if mode.State {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_state, networkId)
		})
	}

	if mode.Receipts {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_receipts, networkId)
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (cli *Client) Download() {
	log.Info("Start snapshot downloading")
	torrents := cli.Cli.Torrents()
	for i := range torrents {
		t := torrents[i]
		go func(t *torrent.Torrent) {
			t.AllowDataDownload()
			t.DownloadAll()

			tt := time.Now()
			prev:=t.BytesCompleted()
		dwn:
			for {
				if t.Info().TotalLength()-t.BytesCompleted() == 0 {
					log.Info("Dowloaded", "snapshot", t.Name(), "t", time.Since(tt))
					break dwn
				} else {
					stats := t.Stats()
					log.Info("Downloading snapshot",
						"snapshot", t.Name(),
						"%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),
						"mb", t.BytesCompleted()/1024/1024,
						"diff(kb)", (t.BytesCompleted()-prev)/1024,
						"seeders", stats.ConnectedSeeders,
						"active", stats.ActivePeers,
						"total", stats.TotalPeers)
					prev = t.BytesCompleted()
					time.Sleep(time.Second*10)

				}

			}
		}(t)
	}
	cli.Cli.WaitAll()

	for _, t := range cli.Cli.Torrents() {
		log.Info("Snapshot seeding", "name", t.Name(), "seeding", t.Seeding())
	}
}

func (cli *Client) GetSnapshots(db ethdb.Database, networkID uint64) (map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo, error) {
	mp := make(map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo)
	networkIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(networkIDBytes, networkID)
	err := db.Walk(dbutils.SnapshotInfoBucket, append(networkIDBytes, []byte(SnapshotInfoHashPrefix)...), 8*8+16, func(k, v []byte) (bool, error) {
		var hash metainfo.Hash
		if len(v) != metainfo.HashSize {
			return true, nil
		}
		copy(hash[:], v)
		t, ok := cli.Cli.Torrent(hash)
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
			SnapshotBlock: snapshotsync.SnapshotBlock,
			Dbpath:        filepath.Join(cli.snapshotsDir, t.Files()[0].Path()),
		}
		mp[snapshotsync.SnapshotType(tp)] = val
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return mp, nil
}

func (cli *Client) SeedSnapshot(name string, path string) (metainfo.Hash, error) {
	info,err:=BuildInfoBytesForSnapshot(path, LmdbFilename)
	if err!=nil {
		return [20]byte{}, err
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return [20]byte{}, err
	}


	t, err := cli.AddTorrentSpec(name, metainfo.HashBytes(infoBytes), infoBytes)
	if err!=nil {
		return [20]byte{}, err
	}
	return t.InfoHash(), nil
}
func (cli *Client) StopSeeding(hash metainfo.Hash) error {
	t,ok := cli.Cli.Torrent(hash)
	if !ok {
		return nil
	}
	ch:=t.Closed()
	t.Drop()
	<-ch
	return nil
}

func getTorrentSpec(db ethdb.Database, snapshotName string, networkID uint64) ([]byte, []byte, error) {
	var infohash, infobytes []byte
	var err error
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	infohash, err = db.GetOne(dbutils.SnapshotInfoBucket, MakeInfoHashKey(snapshotName, networkID))
	if err != nil {
		return nil, nil, err
	}
	infobytes, err = db.GetOne(dbutils.SnapshotInfoBucket, MakeInfoBytesKey(snapshotName, networkID))
	if err != nil {
		return nil, nil, err
	}

	return infohash, infobytes, nil
}
func saveTorrentSpec(db ethdb.Putter, snapshotName string, networkID uint64, hash torrent.InfoHash, infobytes []byte) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	err := db.Put(dbutils.SnapshotInfoBucket, MakeInfoHashKey(snapshotName, networkID), hash.Bytes())
	if err != nil {
		return err
	}
	return db.Put(dbutils.SnapshotInfoBucket, MakeInfoBytesKey(snapshotName, networkID), infobytes)
}

func MakeInfoHashKey(snapshotName string, networkID uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	return append(b, []byte(SnapshotInfoHashPrefix+snapshotName)...)
}

func MakeInfoBytesKey(snapshotName string, networkID uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	return append(b, []byte(SnapshotInfoBytesPrefix+snapshotName)...)
}

// ParseInfoHashKey returns networkID and snapshot name
func ParseInfoHashKey(k []byte) (uint64, string) {
	return binary.BigEndian.Uint64(k), string(bytes.TrimPrefix(k[8:], []byte(SnapshotInfoHashPrefix)))
}
