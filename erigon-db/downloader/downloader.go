// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package downloader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/chansync"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/anacrolix/torrent/webseed"
	"github.com/c2h5oh/datasize"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	// Make Go expvars available to Prometheus for diagnostics.
	_ "github.com/anacrolix/missinggo/v2/expvar-prometheus"
	"github.com/anacrolix/missinggo/v2/panicif"

	g "github.com/anacrolix/generics"
	"golang.org/x/sync/errgroup"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"

	"github.com/erigontech/erigon-db/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/snaptype"
)

var debugWebseed = false

func init() {
	_, debugWebseed = os.LookupEnv("DOWNLOADER_DEBUG_WEBSEED")
	webseed.PrintDebug = debugWebseed
}

// Downloader - component which downloading historical files. Can use BitTorrent, or other protocols
type Downloader struct {
	addWebSeedOpts []torrent.AddWebSeedsOpt
	torrentClient  *torrent.Client

	cfg *downloadercfg.Cfg

	torrentStorage storage.ClientImplCloser

	ctx          context.Context
	stopMainLoop context.CancelFunc
	wg           sync.WaitGroup

	// TODO: Add an implicit prefix to messages from this.
	logger    log.Logger
	verbosity log.Lvl
	// Whether to log seeding (for snap downloaders I think).
	logSeeding bool
	// Reset the log interval after making new requests.
	resetLogInterval chansync.BroadcastCond

	torrentFS *AtomicTorrentFS

	logPrefix string
	// Set when the downloader discovers something isn't complete. Probably doesn't belong in the
	// Downloader.
	startTime time.Time

	lock sync.RWMutex
	// Files having extra verification due to file size mismatches. Use atomics for this map since
	// locking requirements are mixed, and it's not coupled to anything else.
	filesBeingVerified    *xsync.Map[*torrent.File, struct{}]
	verificationOccurring chansync.Flag
	// Torrents that block completion. They were requested specifically. Torrents added from disk
	// that aren't subsequently requested are not required to satisfy the sync stage.
	// https://github.com/erigontech/erigon/issues/15514
	requiredTorrents map[*torrent.Torrent]struct{}
	// The "name" or file path of a torrent is used throughout as a unique identifier for the
	// torrent. Make it explicit rather than fold it into DisplayName or guess at the name at
	// various points. This might change if multi-file torrents are used.
	torrentsByName map[string]*torrent.Torrent
	stats          AggStats
}

// Sets the log interval low again after making new requests.
func (me *Downloader) ResetLogInterval() {
	me.resetLogInterval.Broadcast()
}

type AggStats struct {
	// When these stats were generated.
	When time.Time

	MetadataReady, FilesTotal int
	NumTorrents               int
	PeersUnique               int32
	ConnectionsTotal          uint64

	TorrentsCompleted int

	// BytesCompleted is calculated from Torrent.BytesCompleted which counts dirty bytes which can
	// be failed and so go back down... We're not just using download speed because there are other
	// bottlenecks like hashing.
	BytesCompleted, BytesTotal uint64
	CompletionRate             uint64

	BytesDownload, BytesUpload uint64
	UploadRate, DownloadRate   uint64
	LocalFileHashes            int
	LocalFileHashTime          time.Duration

	BytesHashed, BytesFlushed uint64
	HashRate, FlushRate       uint64

	WebseedActiveTrips    *atomic.Int64
	WebseedMaxActiveTrips *atomic.Int64
	WebseedTripCount      *atomic.Int64
	WebseedDiscardCount   *atomic.Int64
	WebseedServerFails    *atomic.Int64
	WebseedBytesDownload  *atomic.Int64
}

func (me *AggStats) AllTorrentsComplete() bool {
	return me.TorrentsCompleted == me.NumTorrents
}

type requestHandler struct {
	http.Transport
	downloader *Downloader
}

var cloudflareHeaders = http.Header{
	"lsjdjwcush6jbnjj3jnjscoscisoc5s": []string{"I%OSJDNFKE783DDHHJD873EFSIVNI7384R78SSJBJBCCJBC32JABBJCBJK45"},
}

func insertCloudflareHeaders(req *http.Request) {
	for key, value := range cloudflareHeaders {
		req.Header[key] = value
	}
}

// TODO(anacrolix): Upstream any logic that works reliably.
func (r *requestHandler) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	r.downloader.lock.RLock()
	// Peak
	webseedMaxActiveTrips := r.downloader.stats.WebseedMaxActiveTrips
	webseedActiveTrips := r.downloader.stats.WebseedActiveTrips
	webseedTripCount := r.downloader.stats.WebseedTripCount
	webseedBytesDownload := r.downloader.stats.WebseedBytesDownload
	webseedDiscardCount := r.downloader.stats.WebseedDiscardCount
	WebseedServerFails := r.downloader.stats.WebseedServerFails
	r.downloader.lock.RUnlock()

	activeTrips := webseedActiveTrips.Add(1)
	if activeTrips > webseedMaxActiveTrips.Load() {
		webseedMaxActiveTrips.Store(activeTrips)
	}

	defer func() {
		webseedActiveTrips.Add(-1)
	}()

	insertCloudflareHeaders(req)

	webseedTripCount.Add(1)
	resp, err = r.Transport.RoundTrip(req)
	if err != nil {
		return
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if req.Header.Get("Range") != "" {
			// the torrent lib is expecting http.StatusPartialContent so it will discard this
			// if this count is higher than 0, it's likely there is a server side config issue
			// as it implies that the server is not handling range requests correctly and is just
			// returning the whole file - which the torrent lib can't handle
			//
			// TODO: We could count the bytes - probably need to take this from the req though
			// as its not clear the amount of the content which will be read.  This needs
			// further investigation - if required.
			webseedDiscardCount.Add(1)
		}

		webseedBytesDownload.Add(resp.ContentLength)

	// the first two statuses here have been observed from cloudflare
	// during testing.  The remainder are generally understood to be
	// retry-able http responses, calcBackoff will use the Retry-After
	// header if it's available
	case http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusRequestTimeout, http.StatusTooEarly,
		http.StatusTooManyRequests, http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:

		if debugWebseed {
			// An interesting error that the torrent lib should probably handle.
			fmt.Printf("got webseed response status %v\n", resp.Status)
		}

		WebseedServerFails.Add(1)
	default:
		webseedBytesDownload.Add(resp.ContentLength)
	}

	return resp, err
}

func New(ctx context.Context, cfg *downloadercfg.Cfg, logger log.Logger, verbosity log.Lvl) (*Downloader, error) {
	// Cloudflare, or OS socket overhead seems to limit us to ~100-150MB/s in testing to Cloudflare
	// buckets. If we could limit HTTP requests to 1 per connection we'd do that, but the HTTP2
	// config field doesn't do anything yet in Go 1.24 (and 1.25rc1). Disabling HTTP2 is another way
	// to achieve this.
	requestHandler := requestHandler{
		Transport: http.Transport{
			ReadBufferSize: 64 << 10,
			// Note this does nothing in go1.24.
			//HTTP2: &http.HTTP2Config{
			//	MaxConcurrentStreams: 1,
			//},
			// Big hammer to achieve one request per connection.
			//DisableKeepAlives: true,
		},
	}

	// Disable HTTP2. See above.
	//g.MakeMap(&requestHandler.Transport.TLSNextProto)

	// TODO: Add this specifically for webseeds and not as the Client wide HTTP transport.
	cfg.ClientConfig.WebTransport = &requestHandler

	db, err := openMdbx(ctx, cfg.Dirs.Downloader, cfg.MdbxWriteMap)
	if err != nil {
		err = fmt.Errorf("opening downloader mdbx: %w", err)
		return nil, err
	}
	defer db.Close()

	var addWebSeedOpts []torrent.AddWebSeedsOpt

	for value := range cfg.SeparateWebseedDownloadRateLimit.Iter() {
		addWebSeedOpts = append(
			addWebSeedOpts,
			torrent.WebSeedResponseBodyRateLimiter(rate.NewLimiter(value, 0)),
		)
		if value == 0 {
			cfg.ClientConfig.DisableWebseeds = true
		}
	}

	m, torrentClient, err := newTorrentClient(ctx, cfg.Dirs.Snap, cfg.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("newTorrentClient: %w", err)
	}

	peerID, err := readPeerID(db)
	if err != nil {
		return nil, fmt.Errorf("get peer id: %w", err)
	}
	cfg.ClientConfig.PeerID = string(peerID)
	if len(peerID) == 0 {
		if err = savePeerID(db, torrentClient.PeerID()); err != nil {
			return nil, fmt.Errorf("save peer id: %w", err)
		}
	}

	stats := AggStats{
		WebseedActiveTrips:    &atomic.Int64{},
		WebseedMaxActiveTrips: &atomic.Int64{},
		WebseedTripCount:      &atomic.Int64{},
		WebseedBytesDownload:  &atomic.Int64{},
		WebseedDiscardCount:   &atomic.Int64{},
		WebseedServerFails:    &atomic.Int64{},
	}

	d := &Downloader{
		cfg:                cfg,
		torrentStorage:     m,
		torrentClient:      torrentClient,
		addWebSeedOpts:     addWebSeedOpts,
		stats:              stats,
		logger:             logger,
		verbosity:          verbosity,
		torrentFS:          &AtomicTorrentFS{dir: cfg.Dirs.Snap},
		filesBeingVerified: xsync.NewMap[*torrent.File, struct{}](),
	}

	d.logTorrentClientParams()

	if len(cfg.WebSeedUrls) == 0 {
		logger.Warn("downloader has no webseed urls configured")
	}

	requestHandler.downloader = d

	d.ctx, d.stopMainLoop = context.WithCancel(context.Background())

	if d.cfg.AddTorrentsFromDisk {
		d.spawn(func() {
			err := d.AddTorrentsFromDisk(d.ctx)
			if err == nil || ctx.Err() != nil {
				return
			}
			log.Error("error adding torrents from disk", "err", err)
		})
	}

	return d, nil
}

// This should be called synchronously after Downloader.New and probably before adding
// torrents/requests. However, I call it based on the existing config field for now.
func (d *Downloader) AddTorrentsFromDisk(ctx context.Context) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	// Does WalkDir do path or filepath?
	return fs.WalkDir(
		os.DirFS(d.SnapDir()),
		".",
		func(path string, de fs.DirEntry, err error) error {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if err != nil {
				d.logger.Warn("error walking snapshots dir", "path", path, "err", err)
				return nil
			}
			if de.IsDir() {
				return nil
			}
			// Don't need relative file path here because we did it with os.DirFS.
			name, ok := strings.CutSuffix(path, ".torrent")
			if !ok {
				return nil
			}
			_, err = d.addPreverifiedTorrent(g.None[metainfo.Hash](), name)
			if err != nil {
				err = fmt.Errorf("adding torrent for %v: %w", path, err)
				return err
			}
			return nil
		},
	)
}

// This should only be called once...?
func (d *Downloader) MainLoopInBackground(logSeeding bool) {
	d.spawn(func() {
		// Given this should only be called once, set this locally until clarified. Race detector
		// will notice if it's done in poor taste.
		d.logSeeding = logSeeding
		if err := d.loggerRoutine(); err != nil {
			if !errors.Is(err, context.Canceled) {
				d.logger.Warn("[snapshots]", "err", err)
			}
		}
	})
}

func (d *Downloader) loggerRoutine() error {
restart:
	nextLog := time.Now()
	step := time.Second
	reset := d.resetLogInterval.Signaled()
	for {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-time.After(time.Until(nextLog)):
			d.messyLogWrapper()
			nextLog = nextLog.Add(step)
			step = min(step*2, time.Minute)
		case <-reset:
			goto restart
		}
	}
}

func (d *Downloader) messyLogWrapper() {
	d.ReCalcStats()
	if !d.stats.AllTorrentsComplete() {
		d.logProgress()
	}

	// Or files==0?
	if d.logSeeding {
		return
	}

	stats := d.Stats()

	var m runtime.MemStats
	dbg.ReadMemStats(&m)

	if stats.AllTorrentsComplete() && stats.FilesTotal > 0 {
		d.logger.Info("[snapshots] Seeding",
			"up", common.ByteCount(stats.UploadRate)+"/s",
			"peers", stats.PeersUnique,
			"conns", stats.ConnectionsTotal,
			"files", stats.FilesTotal,
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
		return
	}

	if stats.PeersUnique == 0 {
		ips := d.TorrentClient().BadPeerIPs()
		if len(ips) > 0 {
			d.logger.Info("[snapshots] Stats", "banned", ips)
		}
	}
}

func (d *Downloader) SnapDir() string { return d.cfg.Dirs.Snap }

func (d *Downloader) allTorrentsComplete() (ret bool) {
	ret = true
	for _, t := range d.torrentClient.Torrents() {
		if !t.Complete().Bool() {
			if g.MapContains(d.requiredTorrents, t) {
				ret = false
			}
			continue
		}
		// Keep going even if this fails, because we want to trigger piece verification for all
		// torrents that fail. We also validate torrents that weren't explicitly requested, because
		// behaviour might depend on those. If they fail validation they might get made into part
		// files again and protect the corruption from spreading.
		if !d.validateCompletedSnapshot(t) {
			if g.MapContains(d.requiredTorrents, t) {
				ret = false
			}
		}
		// TODO: Should we write the torrent files here instead of in the goroutine spawned in addTorrentSpec?
	}
	// Require that we're not waiting on any file verification. NB this isn't just requiredTorrent
	// pieces, but that might be best.
	if d.filesBeingVerified.Size() != 0 {
		ret = false
	}
	return
}

// Basic checks and fixes for a snapshot torrent claiming it's complete from experiments. If passed
// is false, come back later and check again. You could ask why this isn't in the torrent lib. This
// is an extra level of pedantry due to some file modification I saw from outside the torrent lib.
// It may go away with only writing torrent files and preverified after completion.
func (d *Downloader) validateCompletedSnapshot(t *torrent.Torrent) (passed bool) {
	passed = true
	// This has to be available if it's complete.
	for _, f := range t.Files() {
		// Technically this works, we're preemptively handling multifile torrents here.
		fp := d.filePathForName(f.Path())
		fi, err := os.Stat(fp)
		if err == nil {
			if fi.Size() == f.Length() {
				continue
			}
			d.logger.Crit(
				"snapshot file has wrong size",
				"name", f.Path(),
				"expected", f.Length(),
				"actual", fi.Size(),
			)
			if fi.Size() > f.Length() {
				// This isn't concurrent-safe?
				os.Chmod(fp, 0o644)
				err = os.Truncate(fp, f.Length())
				if err != nil {
					d.logger.Crit("error truncating oversize snapshot file", "name", f.Path(), "err", err)
				}
				os.Chmod(fp, 0o444)
				// End not concurrent safe
			}
		} else {
			// In Erigon 3.1, .torrent files are only written when the data is complete.
			d.logger.Crit("torrent file is present but data is incomplete", "name", f.Path(), "err", err)
		}
		passed = false
		d.verifyFile(f)
	}
	return
}

// Run verification for pieces of the file that aren't already being verified.
func (d *Downloader) verifyFile(f *torrent.File) {
	_, loaded := d.filesBeingVerified.LoadOrStore(f, struct{}{})
	if loaded {
		return
	}
	d.updateVerificationOccurring()
	d.spawn(func() {
		for p := range f.Pieces() {
			err := p.VerifyDataContext(d.ctx)
			if d.ctx.Err() != nil {
				return
			}
			panicif.Err(err)
		}
		_, loaded := d.filesBeingVerified.LoadAndDelete(f)
		// We should be the ones removing this value.
		panicif.False(loaded)
		d.updateVerificationOccurring()
	})
}

// Interval is how long between recalcs.
func (d *Downloader) ReCalcStats() {
	d.lock.RLock()
	prevStats := d.stats
	d.lock.RUnlock()

	stats := d.newStats(prevStats)

	d.lock.Lock()
	d.stats = stats
	d.lock.Unlock()

	if !stats.AllTorrentsComplete() {
		log.Debug("[snapshots] downloading",
			"len", stats.NumTorrents,
			"hashed", common.ByteCount(stats.BytesHashed),
			"hash-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.HashRate)),
			"completed", common.ByteCount(stats.BytesCompleted),
			"completion-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.CompletionRate)),
			"flushed", common.ByteCount(stats.BytesFlushed),
			"flush-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.FlushRate)),
			"downloaded", common.ByteCount(stats.BytesDownload),
			"download-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.DownloadRate)),
			"webseed-trips", stats.WebseedTripCount.Load(),
			"webseed-active", stats.WebseedActiveTrips.Load(),
			"webseed-max-active", stats.WebseedMaxActiveTrips.Load(),
			"webseed-discards", stats.WebseedDiscardCount.Load(),
			"webseed-fails", stats.WebseedServerFails.Load(),
			"webseed-bytes", common.ByteCount(uint64(stats.WebseedBytesDownload.Load())))
	}
}

// Interval is how long between recalcs.
func (d *Downloader) newStats(prevStats AggStats) AggStats {
	torrentClient := d.torrentClient
	peers := make(map[torrent.PeerID]struct{}, 16)
	stats := prevStats
	logger := d.logger
	verbosity := d.verbosity

	// Call these methods outside `lock` critical section, because they have own locks with contention.
	torrents := torrentClient.Torrents()
	connStats := torrentClient.Stats()

	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())
	stats.BytesHashed = uint64(connStats.BytesHashed.Int64())
	stats.BytesDownload = uint64(connStats.BytesReadData.Int64())

	stats.BytesCompleted = 0
	stats.BytesTotal, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0
	stats.TorrentsCompleted = 0
	stats.NumTorrents = len(torrents)

	var noMetadata []string

	isDiagEnabled := diagnostics.TypeOf(diagnostics.SnapshoFilesList{}).Enabled()
	if isDiagEnabled {
		filesList := make([]string, 0, len(torrents))
		for _, t := range torrents {
			filesList = append(filesList, t.Name())
		}
		diagnostics.Send(diagnostics.SnapshoFilesList{Files: filesList})
	}

	for _, t := range torrents {
		stats.BytesCompleted += uint64(t.BytesCompleted())
		select {
		case <-t.GotInfo():
		default: // if some torrents have no metadata, we are for-sure incomplete
			noMetadata = append(noMetadata, t.Name())
			continue
		}
		stats.FilesTotal += len(t.Files())

		torrentName := t.Name()
		torrentComplete := t.Complete().Bool()
		stats.MetadataReady++

		// call methods once - to reduce internal mutex contention
		peersOfThisFile := t.PeerConns()
		weebseedPeersOfThisFile := t.WebseedPeerConns()

		tLen := t.Length()
		var bytesCompleted int64

		if torrentComplete {
			stats.TorrentsCompleted++
			bytesCompleted = tLen
		} else {
			bytesCompleted = t.BytesCompleted()
		}

		progress := float32(float64(100) * (float64(bytesCompleted) / float64(tLen)))

		stats.BytesTotal += uint64(tLen)

		for _, peer := range peersOfThisFile {
			stats.ConnectionsTotal++
			peers[peer.PeerID] = struct{}{}
		}

		webseedRates, webseeds := getWebseedsRatesForlogs(weebseedPeersOfThisFile, torrentName, t.Complete().Bool())
		rates, peers := getPeersRatesForlogs(peersOfThisFile, torrentName)

		// more detailed statistic: download rate of each peer (for each file)
		if !torrentComplete && progress != 0 {
			logger.Log(verbosity, "[snapshots] progress", "file", torrentName, "progress", fmt.Sprintf("%.2f%%", progress), "peers", len(peersOfThisFile), "webseeds", len(weebseedPeersOfThisFile))
			logger.Log(verbosity, "[snapshots] webseed peers", webseedRates...)
			logger.Log(verbosity, "[snapshots] bittorrent peers", rates...)
		}

		diagnostics.Send(diagnostics.SegmentDownloadStatistics{
			Name:            torrentName,
			TotalBytes:      uint64(tLen),
			DownloadedBytes: uint64(bytesCompleted),
			Webseeds:        webseeds,
			Peers:           peers,
		})

	}

	if len(noMetadata) > 0 {
		amount := len(noMetadata)
		if len(noMetadata) > 5 {
			noMetadata = append(noMetadata[:5], "...")
		}
		logger.Info("[snapshots] no metadata yet", "files", amount, "list", strings.Join(noMetadata, ","))
	}

	stats.When = time.Now()
	interval := stats.When.Sub(prevStats.When)
	stats.DownloadRate = calculateRate(stats.BytesDownload, prevStats.BytesDownload, prevStats.DownloadRate, interval)
	stats.HashRate = calculateRate(stats.BytesHashed, prevStats.BytesHashed, prevStats.HashRate, interval)
	stats.FlushRate = calculateRate(stats.BytesFlushed, prevStats.BytesFlushed, prevStats.FlushRate, interval)
	stats.UploadRate = calculateRate(stats.BytesUpload, prevStats.BytesUpload, prevStats.UploadRate, interval)
	stats.CompletionRate = calculateRate(stats.BytesCompleted, prevStats.BytesCompleted, prevStats.CompletionRate, interval)

	stats.PeersUnique = int32(len(peers))
	stats.FilesTotal = len(torrents)

	return stats
}

// Calculating rate with decay in order to avoid rate spikes
func calculateRate(current, previous uint64, prevRate uint64, interval time.Duration) uint64 {
	if interval == 0 {
		return math.MaxUint64
	}
	if current > previous {
		return uint64(time.Second) * (current - previous) / uint64(interval)
	}
	// TODO: Probably assert and find out what is wrong.
	return 0
}

// Adds segment peer fields common to Peer instances.
func setCommonPeerSegmentFields(peer *torrent.Peer, stats *torrent.PeerStats, segment *diagnostics.SegmentPeer) {
	segment.DownloadRate = uint64(stats.DownloadRate)
	segment.UploadRate = uint64(stats.LastWriteUploadRate)
	segment.PiecesCount = uint64(stats.RemotePieceCount)
	segment.RemoteAddr = peer.RemoteAddr.String()
}

func getWebseedsRatesForlogs(weebseedPeersOfThisFile []*torrent.Peer, fName string, finished bool) ([]interface{}, []diagnostics.SegmentPeer) {
	seeds := make([]diagnostics.SegmentPeer, 0, len(weebseedPeersOfThisFile))
	webseedRates := make([]interface{}, 0, len(weebseedPeersOfThisFile)*2)
	webseedRates = append(webseedRates, "file", fName)
	for _, peer := range weebseedPeersOfThisFile {
		if peerUrl, err := webPeerUrl(peer); err == nil {
			if shortUrl, err := url.JoinPath(peerUrl.Host, peerUrl.Path); err == nil {
				stats := peer.Stats()
				if !finished {
					seed := diagnostics.SegmentPeer{
						Url:         peerUrl.Host,
						TorrentName: fName,
					}
					setCommonPeerSegmentFields(peer, &stats, &seed)
					seeds = append(seeds, seed)
				}
				webseedRates = append(
					webseedRates,
					strings.TrimSuffix(shortUrl, "/"),
					fmt.Sprintf("%s/s", common.ByteCount(uint64(stats.DownloadRate))),
				)
			}
		}
	}

	return webseedRates, seeds
}

func webPeerUrl(peer *torrent.Peer) (*url.URL, error) {
	root, _ := path.Split(strings.Trim(strings.TrimPrefix(peer.String(), "webseed peer for "), "\""))
	return url.Parse(root)
}

func getPeersRatesForlogs(peersOfThisFile []*torrent.PeerConn, fName string) ([]interface{}, []diagnostics.SegmentPeer) {
	peers := make([]diagnostics.SegmentPeer, 0, len(peersOfThisFile))
	rates := make([]interface{}, 0, len(peersOfThisFile)*2)
	rates = append(rates, "file", fName)

	for _, peer := range peersOfThisFile {
		url := fmt.Sprintf("%v", peer.PeerClientName.Load())
		stats := peer.Stats()
		segPeer := diagnostics.SegmentPeer{
			Url:         url,
			PeerId:      peer.PeerID,
			TorrentName: fName,
		}
		setCommonPeerSegmentFields(&peer.Peer, &stats, &segPeer)
		peers = append(peers, segPeer)
		rates = append(rates, url, fmt.Sprintf("%s/s", common.ByteCount(uint64(stats.DownloadRate))))
	}

	return rates, peers
}

// Check all loaded torrents by forcing a new verification then checking if the client considers
// them complete. If whitelist is not empty, torrents are verified if their name contains any
// whitelist entry as a prefix, suffix, or total match. TODO: This is too coupled to cmd/Downloader.
func (d *Downloader) VerifyData(
	ctx context.Context,
	whiteList []string,
) error {
	var totalBytes int64
	allTorrents := d.torrentClient.Torrents()
	toVerify := make([]*torrent.Torrent, 0, len(allTorrents))
	for _, t := range allTorrents {
		if t.Info() == nil {
			return fmt.Errorf("%v: missing torrent info", t.Name())
		}
		if len(whiteList) > 0 {
			name := t.Name()
			exactOrPartialMatch := slices.ContainsFunc(whiteList, func(s string) bool {
				return name == s || strings.HasSuffix(name, s) || strings.HasPrefix(name, s)
			})
			if !exactOrPartialMatch {
				continue
			}
		}
		toVerify = append(toVerify, t)
		totalBytes += t.Length()
	}

	d.logger.Info("[snapshots] Verify start")
	defer d.logger.Info("[snapshots] Verify done", "files", len(toVerify), "whiteList", whiteList)

	var (
		verifiedBytes  atomic.Int64
		completedFiles atomic.Uint64
	)

	{
		logEvery := time.NewTicker(20 * time.Second)
		// Make sure this routine stops after we return from this function.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer logEvery.Stop()
		d.spawn(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-logEvery.C:
					d.logger.Info("[snapshots] Verify",
						"progress", fmt.Sprintf("%.2f%%", 100*float64(verifiedBytes.Load())/float64(totalBytes)),
						"files", fmt.Sprintf("%d/%d", completedFiles.Load(), len(toVerify)),
						// GB not GiB?
						"sz_gb", verifiedBytes.Load()>>30,
					)
				}
			}
		})
	}

	eg, ctx := errgroup.WithContext(ctx)
	// We're hashing multiple torrents and the torrent library limits hash concurrency per-torrent.
	// We trigger torrent verification ourselves to make the load more predictable. This will only
	// work if the hashing concurrency is per-torrent (which it is for now). anacrolix/torrent
	// should provide a synchronous hashing mechanism that supports v1/v2. TODO: The multiplier is
	// probably too high now that we don't iterate though pieces.
	eg.SetLimit(runtime.GOMAXPROCS(-1) * 4)
	for _, t := range toVerify {
		verifyTorrentComplete(ctx, eg, t, &verifiedBytes)
		// Technically this requires the pieces for a given torrent to be completed, but I took a
		// shortcut after I realised. I don't think it's necessary for it to be super accurate since
		// we also have a pieces counter.
		completedFiles.Add(1)
	}

	return eg.Wait()
}

// AddNewSeedableFile decides what we do depending on whether we have the .seg file or the .torrent file
// have .torrent no .seg => get .seg file from .torrent
// have .seg no .torrent => get .torrent from .seg
func (d *Downloader) AddNewSeedableFile(ctx context.Context, name string) error {
	ff, isStateFile, ok := snaptype.ParseFileName("", name)
	if ok {
		if isStateFile {
			if !snaptype.E3Seedable(name) {
				return nil
			}
		} else {
			if ff.Type == nil {
				return fmt.Errorf("nil ptr after parsing file: %s", name)
			}
			if !d.cfg.SnapshotConfig.Seedable(ff) {
				return nil
			}
		}
	}

	// if we don't have the torrent file we build it if we have the .seg file
	_, err := BuildTorrentIfNeed(ctx, name, d.SnapDir(), d.torrentFS)
	if err != nil {
		return fmt.Errorf("AddNewSeedableFile: %w", err)
	}
	ts, err := d.torrentFS.LoadByName(name)
	if err != nil {
		return fmt.Errorf("AddNewSeedableFile: %w", err)
	}
	_, _, err = d.addTorrentSpec(ts, name)
	if err != nil {
		return fmt.Errorf("addTorrentSpec: %w", err)
	}
	return nil
}

func (d *Downloader) alreadyHaveThisName(name string) bool {
	return g.MapContains(d.torrentsByName, name)
}

// Loads metainfo from disk, removing it if it's invalid. Returns Some metainfo if it's valid. Logs
// errors.
func (d *Downloader) loadSpecFromDisk(name string) (spec g.Option[*torrent.TorrentSpec]) {
	miPath := d.filePathForName(name) + ".torrent"
	mi, err := metainfo.LoadFromFile(miPath)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	removeMetainfo := func() {
		err := os.Remove(miPath)
		if err != nil {
			d.logger.Error("error removing metainfo file", "err", err, "name", name)
		}
	}
	if err != nil {
		d.logger.Error("loading metainfo from disk", "err", err, "name", name)
		removeMetainfo()
		return
	}
	// TODO: Are we missing a check that the name and the Info.Name match here?
	spec.Set(torrent.TorrentSpecFromMetaInfo(mi))
	return
}

func (d *Downloader) webSeedUrlStrs() iter.Seq[string] {
	return slices.Values(d.cfg.WebSeedUrls)
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk.
func (d *Downloader) RequestSnapshot(
	// The infohash to use if there isn't one on disk. If there isn't one on disk then we can't proceed.
	infoHash metainfo.Hash,
	name string,
) (err error) {
	d.lock.Lock()
	t, err := d.addPreverifiedTorrent(g.Some(infoHash), name)
	if err == nil {
		g.MakeMapIfNil(&d.requiredTorrents)
		g.MapInsert(d.requiredTorrents, t, struct{}{})
	}
	d.lock.Unlock()
	return
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy.
func (d *Downloader) addPreverifiedTorrent(
	// The infohash to use if there isn't one on disk. If there isn't one on disk then we can't proceed.
	infoHashHint g.Option[metainfo.Hash],
	name string,
) (t *torrent.Torrent, err error) {
	diskSpecOpt := d.loadSpecFromDisk(name)
	if !diskSpecOpt.Ok && !infoHashHint.Ok {
		err = errors.New("can't add torrent without infohash")
		return
	}
	if diskSpecOpt.Ok && infoHashHint.Ok && diskSpecOpt.Value.InfoHash != infoHashHint.Value {
		// This is allowed if the torrent file was committed to disk. It's assumed the torrent was
		// downloaded in its entirety with a previous hash. TODO: Should we check there were
		// actually info bytes?
		d.logger.Debug("disk metainfo hash mismatch",
			"expected", infoHashHint,
			"actual", diskSpecOpt.Value.InfoHash,
			"name", name)
	}
	// Prefer the infohash from disk, then the caller's.
	finalInfoHash := func() infohash.T {
		if diskSpecOpt.Ok {
			return diskSpecOpt.Value.InfoHash
		}
		return infoHashHint.Unwrap()
	}()

	ok, err := d.shouldAddTorrent(finalInfoHash, name)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	// TorrentSpec was created before I knew better about Go's heap... Set a default
	spec := diskSpecOpt.UnwrapOr(new(torrent.TorrentSpec))
	// This will trigger a mismatch if info bytes are known and don't match. We would have already
	// failed if the info bytes aren't known.
	spec.InfoHash = finalInfoHash
	spec.DisplayName = cmp.Or(spec.DisplayName, name)
	spec.Sources = nil
	for s := range d.webSeedUrlStrs() {
		u := s + name + ".torrent"
		spec.Sources = append(spec.Sources, u)
	}
	t, ok, err = d.addTorrentSpec(spec, name)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	metainfoOnDisk := diskSpecOpt.Ok
	if metainfoOnDisk {
		d.spawn(func() {
			if !d.validateCompletedSnapshot(t) {
				// This is totally recoverable. It's not great if it happens for files that aren't
				// in the current preverified set because we have no guarantees about WebSeeds or
				// other peers.
				d.logger.Warn("torrent metainfo on disk but torrent failed validation", "name", t.Name())
				// Maybe we could replace the torrent with the infoHashHint?
			}
		})
	}

	d.afterAddNewTorrent(metainfoOnDisk, t)
	return
}

// Check the proposed infohash and name combo don't conflict with an existing torrent. Names need to
// be unique, and infohashes are 160bit hashes of data that includes the name...
func (d *Downloader) shouldAddTorrent(
	infoHash metainfo.Hash,
	name string,
) (bool, error) {
	if !IsSnapNameAllowed(name) {
		return false, fmt.Errorf("snap name %q is not allowed", name)
	}
	t, ok := d.torrentClient.Torrent(infoHash)
	if ok {
		if t.Info() == nil {
			d.logger.Warn("infohash already added but info not obtained. can't verify unique name", "infohash", infoHash, "name", name)
		} else {
			// Assume the info is already obtained. Pretty sure the node won't try to add torrents
			// until previous stages are complete and this means the info is known. This can be
			// changed.
			existingName := t.Info().Name
			if existingName != name {
				return false, fmt.Errorf("torrent with hash %v already exists with name %q", infoHash, existingName)
			}
		}
	} else if d.alreadyHaveThisName(name) {
		return false, fmt.Errorf("name exists with different torrent hash")
	}
	return !ok, nil
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk.
func (d *Downloader) afterAddNewTorrent(metainfoOnDisk bool, t *torrent.Torrent) {
	// TODO: If the metainfo was on disk already, do we want to ensure Info exists?
	d.spawn(func() {
		select {
		case <-d.ctx.Done():
			return
		case <-t.GotInfo():
		}
		// Always download, even if we think we're complete already. Failure to validate, or things
		// changing can result in us needing to be in the download state to repair. It costs nothing
		// if the torrent is already complete.
		t.DownloadAll()
		if !metainfoOnDisk {
			d.saveMetainfoWhenComplete(t)
		}
	})
}

func (d *Downloader) saveMetainfoWhenComplete(t *torrent.Torrent) {
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-t.Complete().On():
		}
		select {
		case <-d.ctx.Done():
			return
		case <-d.verificationOccurring.Off():
			// This could be filtered to only files we care about...
		}
		if func() (done bool) {
			d.lock.Lock()
			defer d.lock.Unlock()
			if !t.Complete().Bool() || d.filesBeingVerified.Size() != 0 {
				return
			}
			err := d.saveMetainfoPrecheck(t)
			if err != nil {
				d.logger.Warn("torrent failed metainfo commit precheck", "err", err, "name", t.Name())
				return
			}
			err = d.saveMetainfo(t)
			if err != nil {
				// This is unrecoverable. We have to give up.
				d.logger.Error("failed to save torrent metainfo", "err", err, "name", t.Name())
			}
			return true
		}() {
			break
		}
	}
}

func (d *Downloader) saveMetainfoPrecheck(t *torrent.Torrent) error {
	if !t.Complete().Bool() {
		return errors.New("torrent is not complete")
	}
	if !d.validateCompletedSnapshot(t) {
		return errors.New("completed torrent failed validation")
	}
	return nil
}

func (d *Downloader) saveMetainfo(t *torrent.Torrent) error {
	mi := t.Metainfo()
	// This checks for existence last I checked, which isn't really what we want.
	created, err := d.torrentFS.CreateWithMetaInfo(t.Info(), &mi)
	if err == nil && !created {
		err = errors.New("metainfo file already exists")
	}
	if err != nil {
		return fmt.Errorf("error creating metainfo file: %w", err)
	}
	return nil
}

func SeedableFiles(dirs datadir.Dirs, chainName string, all bool) ([]string, error) {
	files, err := seedableSegmentFiles(dirs.Snap, chainName, all)
	if err != nil {
		return nil, fmt.Errorf("seedableSegmentFiles: %w", err)
	}
	l1, err := seedableStateFilesBySubDir(dirs.Snap, "idx", all)
	if err != nil {
		return nil, err
	}
	l2, err := seedableStateFilesBySubDir(dirs.Snap, "history", all)
	if err != nil {
		return nil, err
	}
	l3, err := seedableStateFilesBySubDir(dirs.Snap, "domain", all)
	if err != nil {
		return nil, err
	}
	var l4, l5 []string
	if all {
		l4, err = seedableStateFilesBySubDir(dirs.Snap, "accessor", all)
		if err != nil {
			return nil, err
		}
	}
	// check if dirs.SnapCaplin exists
	if _, err := os.Stat(dirs.SnapCaplin); !os.IsNotExist(err) {
		l5, err = seedableSegmentFiles(dirs.SnapCaplin, chainName, all)
		if err != nil {
			return nil, err
		}
	}

	return slices.Concat(files, l1, l2, l3, l4, l5), nil
}

func (d *Downloader) BuildTorrentFilesIfNeed(ctx context.Context, chain string, ignore snapcfg.PreverifiedItems) error {
	_, err := BuildTorrentFilesIfNeed(ctx, d.cfg.Dirs, d.torrentFS, chain, ignore, false)
	return err
}

func (d *Downloader) Stats() AggStats {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.stats
}

func (d *Downloader) Close() {
	d.logger.Info("[snapshots] stopping downloader", "files", len(d.torrentClient.Torrents()))
	d.stopMainLoop()
	d.wg.Wait()
	d.logger.Info("[snapshots] closing torrents")
	d.torrentClient.Close()
	if err := d.torrentStorage.Close(); err != nil {
		d.logger.Warn("[snapshots] torrentStorage.close", "err", err)
	}
	d.logger.Info("[snapshots] downloader stopped")
}

func (d *Downloader) PeerID() []byte {
	peerID := d.torrentClient.PeerID()
	return peerID[:]
}

func (d *Downloader) TorrentClient() *torrent.Client { return d.torrentClient }

// For the downloader only.
func openMdbx(
	ctx context.Context,
	dbDir string,
	writeMap bool,
) (
	db kv.RwDB,
	err error,
) {
	dbCfg := mdbx.New(kv.DownloaderDB, log.New()).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		GrowthStep(16 * datasize.MB).
		MapSize(16 * datasize.GB).
		PageSize(4 * datasize.KB).
		RoTxsLimiter(semaphore.NewWeighted(9_000)).
		Path(dbDir).
		WriteMap(writeMap)
	return dbCfg.Open(ctx)
}

// This used to return the MDBX database. Instead, that's opened separately now and should be passed
// in if it's revived.
func newTorrentClient(
	ctx context.Context,
	snapDir string,
	cfg *torrent.ClientConfig,
) (
	m storage.ClientImplCloser,
	torrentClient *torrent.Client,
	err error,
) {
	// Possibly File storage needs more optimization for handles, will test. Should reduce GC
	// pressure and improve scheduler handling.
	// See also: https://github.com/erigontech/erigon/pull/10074
	// We're using part-file-only piece completion. This requires hashing incomplete files at
	// start-up, but the expectation is that files should not be partial. Also managing separate
	// piece completion complicates user management of snapshots.
	m = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir: snapDir,
		UsePartFiles:  g.Some(true),
		Logger:        cfg.Slogger.With("names", "storage"),
	})
	cfg.DefaultStorage = m

	defer func() {
		if err != nil {
			err = errors.Join(err, m.Close())
		}
	}()

	dnsResolver := &downloadercfg.DnsCacheResolver{RefreshTimeout: 24 * time.Hour}
	cfg.TrackerDialContext = dnsResolver.DialContext

	torrentClient, err = torrent.NewClient(cfg)
	if err != nil {
		err = fmt.Errorf("creating torrent client: %w", err)
		return
	}

	go func() {
		dnsResolver.Run(ctx)
	}()

	return
}

// Moved from EthConfig to be as close to torrent client init as possible. Logs important torrent
// parameters.
func (d *Downloader) logTorrentClientParams() {
	cfg := d.cfg.ClientConfig
	d.logger.Info(
		"[Downloader] Running with",
		"ipv6-enabled", !cfg.DisableIPv6,
		"ipv4-enabled", !cfg.DisableIPv4,
		"download.rate", rateLimitString(cfg.DownloadRateLimiter.Limit()),
		"webseed-download-rate", func() string {
			opt := d.cfg.SeparateWebseedDownloadRateLimit
			if opt.Ok {
				return rateLimitString(opt.Value)
			}
			return "shared with p2p"
		}(),
		"upload.rate", rateLimitString(cfg.UploadRateLimiter.Limit()),
	)
}

func rateLimitString(rateLimit rate.Limit) string {
	if rateLimit == rate.Inf {
		return "∞"
	}
	// Use the same type for human-readable bytes as elsewhere.
	return datasize.ByteSize(rateLimit).String()
}

func (d *Downloader) SetLogPrefix(prefix string) {
	// Data race?
	d.logPrefix = prefix
}

// Currently only called if not all torrents are complete.
func (d *Downloader) logProgress() {
	var m runtime.MemStats
	prefix := d.logPrefix

	if d.logPrefix == "" {
		prefix = "snapshots"
	}

	dbg.ReadMemStats(&m)

	bytesDone := d.stats.BytesCompleted

	percentDone := float32(100) * (float32(bytesDone) / float32(d.stats.BytesTotal))
	rate := d.stats.CompletionRate
	remainingBytes := d.stats.BytesTotal - bytesDone

	timeLeft := calculateTime(remainingBytes, rate)

	haveAllMetadata := d.stats.MetadataReady == d.stats.NumTorrents

	if !d.stats.AllTorrentsComplete() {
		// We have work to do so start timing.
		d.setStartTime()
		// TODO: Include what we're syncing.
		log.Info(fmt.Sprintf("[%s] Syncing", prefix),
			"file-metadata", fmt.Sprintf("%d/%d", d.stats.MetadataReady, d.stats.NumTorrents),
			"files", fmt.Sprintf(
				"%d/%d",
				// For now it's 1:1 files:torrents.
				d.stats.TorrentsCompleted,
				d.stats.NumTorrents,
			),
			"data", func() string {
				if haveAllMetadata {
					return fmt.Sprintf(
						"%.2f%% - %s/%s",
						percentDone,
						common.ByteCount(bytesDone),
						common.ByteCount(d.stats.BytesTotal),
					)
				} else {
					return common.ByteCount(bytesDone)
				}
			}(),
			"time-left", timeLeft,
			"total-time", time.Since(d.startTime).Truncate(time.Second).String(),
			"download-rate", fmt.Sprintf("%s/s", common.ByteCount(d.stats.DownloadRate)),
			"hashing-rate", fmt.Sprintf("%s/s", common.ByteCount(d.stats.HashRate)),
			"alloc", common.ByteCount(m.Alloc),
			"sys", common.ByteCount(m.Sys),
		)
	}

	diagnostics.Send(diagnostics.SnapshotDownloadStatistics{
		Downloaded:           bytesDone,
		Total:                d.stats.BytesTotal,
		TotalTime:            time.Since(d.startTime).Round(time.Second).Seconds(),
		DownloadRate:         d.stats.DownloadRate,
		UploadRate:           d.stats.UploadRate,
		Peers:                d.stats.PeersUnique,
		Files:                int32(d.stats.FilesTotal),
		Connections:          d.stats.ConnectionsTotal,
		Alloc:                m.Alloc,
		Sys:                  m.Sys,
		DownloadFinished:     d.stats.AllTorrentsComplete(),
		TorrentMetadataReady: int32(d.stats.MetadataReady),
	})
}

func calculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "∞"
	}
	return time.Duration(float64(amountLeft) / float64(rate) * float64(time.Second)).Truncate(time.Second).String()
}

func (d *Downloader) Completed() bool {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.allTorrentsComplete()
}

// Expose torrent client status to HTTP on the public/default serve mux used by GOPPROF=http, and
// the provided "debug" mux if non-nil. Only do this if you have a single instance of a Downloader.
func (d *Downloader) HandleTorrentClientStatus(debugMux *http.ServeMux) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.torrentClient.WriteStatus(w)
	})
	p := "/downloader/torrentClientStatus"
	// This is for gopprof.
	defaultMux := http.DefaultServeMux
	defaultMux.Handle(p, h)
	if debugMux != nil && debugMux != defaultMux {
		debugMux.Handle(p, h)
	}
}

func (d *Downloader) spawn(f func()) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		f()
	}()
}

func (d *Downloader) updateVerificationOccurring() {
	d.verificationOccurring.SetBool(d.filesBeingVerified.Size() != 0)
}

// Delete - stop seeding, remove file, remove .torrent.
func (s *Downloader) Delete(name string) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	t, ok := s.torrentsByName[name]
	if !ok {
		return
	}
	t.Drop()
	err = os.Remove(s.filePathForName(name))
	if err != nil {
		level := log.LvlError
		if errors.Is(err, fs.ErrNotExist) {
			level = log.LvlInfo
		}
		s.logger.Log(level, "error removing snapshot file data", "name", name, "err", err)
	}
	err = s.torrentFS.Delete(name)
	if err != nil {
		s.logger.Log(log.LvlError, "error removing snapshot file torrent", "name", name, "err", err)
	}
	g.MustDelete(s.torrentsByName, name)
	// I wonder if it's an issue if this occurs before initial sync has completed.
	delete(s.requiredTorrents, t)
	return nil
}

func (d *Downloader) filePathForName(name string) string {
	return filepath.Join(d.SnapDir(), filepath.FromSlash(name))
}

// Set the start time for the progress logging. Only set when we determine we're actually starting work.
func (d *Downloader) setStartTime() {
	if d.startTime.IsZero() {
		d.startTime = time.Now()
	}
}
