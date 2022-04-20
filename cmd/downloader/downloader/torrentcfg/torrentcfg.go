package torrentcfg

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/log/v3"
	"go.etcd.io/bbolt"
	"golang.org/x/time/rate"
)

// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
// amount of network announcements, but can't go over 2Mb
// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
const DefaultPieceSize = 2 * 1024 * 1024

func Default() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()

	// enable dht
	torrentConfig.NoDHT = true
	//torrentConfig.DisableTrackers = true
	//torrentConfig.DisableWebtorrent = true
	//torrentConfig.DisableWebseeds = true

	// Increase default timeouts, because we often run on commodity networks
	torrentConfig.MinDialTimeout = 6 * time.Second      // default: 3sec
	torrentConfig.NominalDialTimeout = 20 * time.Second // default: 20sec
	torrentConfig.HandshakesTimeout = 8 * time.Second   // default: 4sec

	return torrentConfig
}

func New(snapshotsDir *dir.Rw, verbosity lg.Level, natif nat.Interface, downloadRate, uploadRate datasize.ByteSize, port, maxPeers, connsPerFile int, db kv.RwDB) (*torrent.ClientConfig, io.Closer, error) {
	torrentConfig := Default()
	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50
	torrentConfig.TorrentPeersHighWater = maxPeers          // default: 500
	torrentConfig.TorrentPeersLowWater = 5                  // default: 50
	torrentConfig.HalfOpenConnsPerTorrent = 5               // default: 25
	torrentConfig.TotalHalfOpenConns = 10                   // default: 100

	torrentConfig.ListenPort = port
	torrentConfig.Seed = true
	torrentConfig.DataDir = snapshotsDir.Path
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"

	switch natif.(type) {
	case nil:
		// No NAT interface, do nothing.
	case nat.ExtIP:
		// ExtIP doesn't block, set the IP right away.
		ip, _ := natif.ExternalIP()
		torrentConfig.PublicIp4 = ip
		log.Info("[torrent] Public IP", "ip", torrentConfig.PublicIp4)
		// how to set ipv6?
		//torrentConfig.PublicIp6 = net.ParseIP(ip)

	default:
		// Ask the router about the IP. This takes a while and blocks startup,
		// do it in the background.
		if ip, err := natif.ExternalIP(); err == nil {
			torrentConfig.PublicIp4 = ip
			log.Info("[torrent] Public IP", "ip", torrentConfig.PublicIp4)
		}
	}
	// rates are divided by 2 - I don't know why it works, maybe bug inside torrent lib accounting
	torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadRate.Bytes()/2), 2*DefaultPieceSize)     // default: unlimited
	torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadRate.Bytes()/2), 2*DefaultPieceSize) // default: unlimited

	// debug
	if lg.Debug == verbosity {
		torrentConfig.Debug = true
	}
	torrentConfig.Logger = lg.Default.FilterLevel(verbosity)
	torrentConfig.Logger.Handlers = []lg.Handler{adapterHandler{}}

	c, err := NewBoltPieceCompletion(db)
	if err != nil {
		return nil, nil, fmt.Errorf("NewBoltPieceCompletion: %w", err)
	}
	m := storage.NewMMapWithCompletion(snapshotsDir.Path, c)
	torrentConfig.DefaultStorage = m
	return torrentConfig, m, nil
}

const (
	boltDbCompleteValue   = "c"
	boltDbIncompleteValue = "i"
)

var completionBucketKey = []byte("completion")

type boltPieceCompletion struct {
	db kv.RwDB
}

var _ storage.PieceCompletion = (*boltPieceCompletion)(nil)

func NewBoltPieceCompletion(db kv.RwDB) (ret storage.PieceCompletion, err error) {
	ret = &boltPieceCompletion{db}
	return
}

func (me boltPieceCompletion) Get(pk metainfo.PieceKey) (cn storage.Completion, err error) {
	err = me.db.View(func(tx kv.Tx) error {
		cb, _ := tx.Cursor(kv.Snapshot)
		if cb == nil {
			return nil
		}
		ih := cb.Bucket(pk.InfoHash[:])
		if ih == nil {
			return nil
		}
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(pk.Index))
		cn.Ok = true
		switch string(ih.Get(key[:])) {
		case boltDbCompleteValue:
			cn.Complete = true
		case boltDbIncompleteValue:
			cn.Complete = false
		default:
			cn.Ok = false
		}
		return nil
	})
	return
}

func (me boltPieceCompletion) Set(pk metainfo.PieceKey, b bool) error {
	if c, err := me.Get(pk); err == nil && c.Ok && c.Complete == b {
		return nil
	}
	return me.db.Update(func(tx *bbolt.Tx) error {
		c, err := tx.CreateBucketIfNotExists(completionBucketKey)
		if err != nil {
			return err
		}
		ih, err := c.CreateBucketIfNotExists(pk.InfoHash[:])
		if err != nil {
			return err
		}
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(pk.Index))
		return ih.Put(key[:], []byte(func() string {
			if b {
				return boltDbCompleteValue
			} else {
				return boltDbIncompleteValue
			}
		}()))
	})
}

func (me *boltPieceCompletion) Close() error {
	me.db.Close()
	return nil
}
