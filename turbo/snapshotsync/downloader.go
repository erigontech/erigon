package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/ledgerwatch/erigon-lib/kv"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	Cli          *torrent.Client
	snapshotsDir string
	trackers     [][]string
}

func New(snapshotsDir string, seeding bool, peerID string) (*Client, error) {
	torrentConfig := DefaultTorrentConfig()
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"
	torrentConfig.PeerID = peerID

	torrentClient, err := torrent.NewClient(torrentConfig)
	if err != nil {
		log.Error("Fail to start torrnet client", "err", err)
		return nil, fmt.Errorf("fail to start: %w", err)
	}

	return &Client{
		Cli:          torrentClient,
		snapshotsDir: snapshotsDir,
		trackers:     Trackers,
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
	t := cli.Cli.Torrents()
	hashes := make([]metainfo.Hash, 0, len(t))
	for _, v := range t {
		hashes = append(hashes, v.InfoHash())
	}
	return hashes
}
func (cli *Client) Load(tx kv.Tx) error {
	log.Info("Load added torrents")
	return tx.ForEach(kv.SnapshotInfo, []byte{}, func(k, infoHashBytes []byte) error {
		if !bytes.HasPrefix(k[8:], []byte(SnapshotInfoHashPrefix)) {
			return nil
		}
		networkID, snapshotName := ParseInfoHashKey(k)
		infoHash := metainfo.Hash{}
		copy(infoHash[:], infoHashBytes)
		infoBytes, err := tx.GetOne(kv.SnapshotInfo, MakeInfoBytesKey(snapshotName, networkID))
		if err != nil {
			return err
		}

		log.Info("Add torrent", "snapshot", snapshotName, "hash", infoHash.String(), "infobytes", len(infoBytes) > 0)
		_, err = cli.AddTorrentSpec(snapshotName, infoHash, infoBytes)
		if err != nil {
			return err
		}

		return nil
	})
}

func (cli *Client) SavePeerID(db kv.Putter) error {
	return db.Put(kv.BittorrentInfo, []byte(kv.BittorrentPeerID), cli.PeerID())
}

func (cli *Client) Close() {
	cli.Cli.Close()
}

func (cli *Client) PeerID() []byte {
	peerID := cli.Cli.PeerID()
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

func (cli *Client) AddTorrent(ctx context.Context, db kv.RwTx, snapshotType SnapshotType, networkID uint64) error { //nolint: interfacer
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

func (cli *Client) AddSnapshotsTorrents(ctx context.Context, db kv.RwTx, networkId uint64, mode SnapshotMode) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	eg := errgroup.Group{}

	if mode.Headers {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, SnapshotType_headers, networkId)
		})
	}

	if mode.Bodies {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, SnapshotType_bodies, networkId)
		})
	}

	if mode.State {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, SnapshotType_state, networkId)
		})
	}

	if mode.Receipts {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, SnapshotType_receipts, networkId)
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
			defer debug.LogPanic()
			t.AllowDataDownload()
			t.DownloadAll()

			tt := time.Now()
			prev := t.BytesCompleted()
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
					time.Sleep(time.Second * 10)

				}

			}
		}(t)
	}
	cli.Cli.WaitAll()

	for _, t := range cli.Cli.Torrents() {
		log.Info("Snapshot seeding", "name", t.Name(), "seeding", t.Seeding())
	}
}

func (cli *Client) GetSnapshots(tx kv.Tx, networkID uint64) (map[SnapshotType]*SnapshotsInfo, error) {
	mp := make(map[SnapshotType]*SnapshotsInfo)
	networkIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(networkIDBytes, networkID)
	err := tx.ForPrefix(kv.SnapshotInfo, append(networkIDBytes, []byte(SnapshotInfoHashPrefix)...), func(k, v []byte) error {
		var hash metainfo.Hash
		if len(v) != metainfo.HashSize {
			return nil
		}
		copy(hash[:], v)
		t, ok := cli.Cli.Torrent(hash)
		if !ok {
			return nil
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
		tp, ok := SnapshotType_value[tpStr]
		if !ok {
			return fmt.Errorf("incorrect type: %v", tpStr)
		}

		val := &SnapshotsInfo{
			Type:          SnapshotType(tp),
			GotInfoByte:   gotInfo,
			Readiness:     readiness,
			SnapshotBlock: SnapshotBlock,
			Dbpath:        filepath.Join(cli.snapshotsDir, t.Files()[0].Path()),
		}
		mp[SnapshotType(tp)] = val
		return nil
	})
	if err != nil {
		return nil, err
	}

	return mp, nil
}

func (cli *Client) SeedSnapshot(name string, path string) (metainfo.Hash, error) {
	info, err := BuildInfoBytesForSnapshot(path, MdbxFilename)
	if err != nil {
		return [20]byte{}, err
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return [20]byte{}, err
	}

	t, err := cli.AddTorrentSpec(name, metainfo.HashBytes(infoBytes), infoBytes)
	if err != nil {
		return [20]byte{}, err
	}
	return t.InfoHash(), nil
}
func (cli *Client) StopSeeding(hash metainfo.Hash) error {
	t, ok := cli.Cli.Torrent(hash)
	if !ok {
		return nil
	}
	ch := t.Closed()
	t.Drop()
	<-ch
	return nil
}

func getTorrentSpec(db kv.Tx, snapshotName string, networkID uint64) ([]byte, []byte, error) {
	var infohash, infobytes []byte
	var err error
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	infohash, err = db.GetOne(kv.SnapshotInfo, MakeInfoHashKey(snapshotName, networkID))
	if err != nil {
		return nil, nil, err
	}
	infobytes, err = db.GetOne(kv.SnapshotInfo, MakeInfoBytesKey(snapshotName, networkID))
	if err != nil {
		return nil, nil, err
	}

	return infohash, infobytes, nil
}
func saveTorrentSpec(db kv.Putter, snapshotName string, networkID uint64, hash torrent.InfoHash, infobytes []byte) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	err := db.Put(kv.SnapshotInfo, MakeInfoHashKey(snapshotName, networkID), hash.Bytes())
	if err != nil {
		return err
	}
	return db.Put(kv.SnapshotInfo, MakeInfoBytesKey(snapshotName, networkID), infobytes)
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

func GetInfo() {

}

func SnapshotSeeding(chainDB kv.RwDB, cli *Client, name string, snapshotsDir string) error {
	var snapshotBlock uint64
	var hasSnapshotBlock bool
	if err := chainDB.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotBlock)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return err
		}
		hasSnapshotBlock = len(v) == 8
		if hasSnapshotBlock {
			snapshotBlock = binary.BigEndian.Uint64(v)
		} else {
			log.Warn("Snapshot block unknown", "snapshot", name, "v", common.Bytes2Hex(v))
		}
		return nil
	}); err != nil {
		return err
	}

	if hasSnapshotBlock {
		hash, err := cli.SeedSnapshot(name, SnapshotName(snapshotsDir, name, snapshotBlock))
		if err != nil {
			return err
		}
		log.Info("Start seeding", "snapshot", name, "hash", hash.String())
	}
	return nil
}
