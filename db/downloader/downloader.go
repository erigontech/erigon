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
	"net"
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

	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/c2h5oh/datasize"
	"github.com/puzpuzpuz/xsync/v4"

	"github.com/anacrolix/chansync"
	g "github.com/anacrolix/generics"
	// Make Go expvars available to Prometheus for diagnostics.
	_ "github.com/anacrolix/missinggo/v2/expvar-prometheus"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/anacrolix/torrent/webseed"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/diagnostics/diaglib"
)

var debugWebseed = false

const TorrentClientStatusPath = "/downloader/torrentClientStatus"

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

	BytesDownload, BytesUpload                                 uint64
	ClientWebseedBytesDownload, ClientWebseedBytesDownloadRate uint64
	PeerConnBytesDownload, PeerConnBytesDownloadRate           uint64
	UploadRate, DownloadRate                                   uint64

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
	http.RoundTripper
	downloader *Downloader
}

var cloudflareHeaders = http.Header{
	"lsjdjwcush6jbnjj3jnjscoscisoc5s": []string{"I%OSJDNFKE783DDHHJD873EFSIVNI7384R78SSJBJBCCJBC32JABBJCBJK45"},
}

func insertCloudflareHeaders(req *http.Request) {
	// Note this is clobbering the headers.
	for key, value := range cloudflareHeaders {
		req.Header[key] = value
	}
}

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (me roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return me(req)
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
	resp, err = r.RoundTripper.RoundTrip(req)
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

var (
	httpDialer = net.Dialer{
		Timeout: time.Minute,
	}
	httpReadBufferSize = initIntFromEnv("DOWNLOADER_HTTP_READ_BUFFER_SIZE", 2<<20, 0)
	maxConnsPerHost    = initIntFromEnv("DOWNLOADER_MAX_CONNS_PER_HOST", 10, 0)
	tcpReadBufferSize  = initIntFromEnv("DOWNLOADER_TCP_READ_BUFFER_SIZE", 0, 0)
	useHttp3           = os.Getenv("DOWNLOADER_HTTP3") != ""
	forceIpv4          = os.Getenv("DOWNLOADER_FORCE_IPV4") != ""
)

// Configure a downloader transport (requests and metainfo sources). These now use common settings.
func makeTransport() http.RoundTripper {
	if useHttp3 {
		return &http3.Transport{}
	}
	t := &http.Transport{
		ReadBufferSize: httpReadBufferSize,
		// Big hammer to achieve one request per connection.
		DisableKeepAlives:     os.Getenv("DOWNLOADER_DISABLE_KEEP_ALIVES") != "",
		ResponseHeaderTimeout: time.Minute,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       10 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
			if forceIpv4 {
				switch network {
				case "tcp", "tcp6":
					network = "tcp4"
				case "tcp4":
				default:
					panic(network)
				}
			}
			conn, err = httpDialer.DialContext(ctx, network, addr)
			if err != nil {
				return
			}
			if tcpReadBufferSize != 0 {
				err = conn.(*net.TCPConn).SetReadBuffer(tcpReadBufferSize)
				panicif.Err(err)
			}
			return
		},
	}
	configureHttp2(t)
	return t
}

// Configures "Downloader" Transport HTTP2.
func configureHttp2(t *http.Transport) {
	if os.Getenv("DOWNLOADER_DISABLE_HTTP2") != "" {
		// Disable h2 being added automatically.
		g.MakeMap(&t.TLSNextProto)
		return
	}
	// Don't set the http2.Transport as the RoundTripper. It's hooked into the http.Transport by
	// this call. Need to use external http2 library to get access to some config fields that
	// aren't in std.
	h2t, err := http2.ConfigureTransports(t)
	panicif.Err(err)
	// Some of these are the defaults, but I really don't trust Go HTTP2 at this point.

	// Will this fix pings from not timing out?
	h2t.WriteByteTimeout = 15 * time.Second
	// If we don't read for this long, send a ping.
	h2t.ReadIdleTimeout = 15 * time.Second
	h2t.PingTimeout = 15 * time.Second
	h2t.MaxReadFrameSize = 1 << 20 // Same as net/http.Transport.ReadBufferSize?
}

func New(ctx context.Context, cfg *downloadercfg.Cfg, logger log.Logger, verbosity log.Lvl) (*Downloader, error) {
	requestHandler := &requestHandler{}
	{
		requestHandler.RoundTripper = makeTransport()
		cfg.ClientConfig.WebTransport = requestHandler
		// requestHandler.downloader is set later.
	}
	{
		metainfoSourcesTransport := makeTransport()
		// Separate transport so webseed requests and metainfo fetching don't block each other.
		// Additionally, we can tune for their specific workloads.
		cfg.ClientConfig.MetainfoSourcesClient = &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				insertCloudflareHeaders(req)
				return metainfoSourcesTransport.RoundTrip(req)
			}),
		}
	}

	db, err := openMdbx(ctx, cfg.Dirs.Downloader, cfg.MdbxWriteMap)
	if err != nil {
		err = fmt.Errorf("opening downloader mdbx: %w", err)
		return nil, err
	}
	defer db.Close()

	var addWebSeedOpts []torrent.AddWebSeedsOpt //nolint:prealloc

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
		if d.cfg.VerifyTorrentData {
			return nil, errors.New("must add torrents from disk synchronously if downloader verify enabled")
		}
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
	if err := fs.WalkDir(
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
	); err != nil {
		return err
	}

	d.afterAdd()
	return nil
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
	var step time.Duration
	reset := d.resetLogInterval.Signaled()
	for {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-time.After(time.Until(nextLog)):
			d.ReCalcStats()
			d.logStats()
			switch s := d.state(); s {
			case Idle, Seeding:
				step = min(max(step*2, time.Minute), time.Hour)
			case Syncing:
				step = min(max(step, time.Second)*2, 30*time.Second)
			default:
				panic(s)
			}
			nextLog = nextLog.Add(step)
		case <-reset:
			goto restart
		}
	}
}

func (d *Downloader) SnapDir() string { return d.cfg.Dirs.Snap }

// TODO: Zero start-time when true. We're done for now. Return true for this on required torrents?
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

// Basic checks and fixes for a snapshot torrent claiming it's complete. If passed is false, come
// back later and check again. You could ask why this isn't in the torrent lib. This is an extra
// level of pedantry due to some file modification I saw from outside the torrent lib. TODO: Revisit
// this now partial files support is stable. Should be sufficient to tell the Client to reverify
// data.
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
			d.logger.Warn(
				"snapshot file has wrong size",
				"name", f.Path(),
				"expected", f.Length(),
				"actual", fi.Size(),
			)
		} else if passed {
			// In Erigon 3.1, .torrent files are only written when the data is complete.
			d.logger.Warn("torrent file is present but data is incomplete", "name", f.Path(), "err", err)
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
			"hash-rate", common.ByteCount(stats.HashRate)+"/s",
			"completed", common.ByteCount(stats.BytesCompleted),
			"completion-rate", common.ByteCount(stats.CompletionRate)+"/s",
			"flushed", common.ByteCount(stats.BytesFlushed),
			"flush-rate", common.ByteCount(stats.FlushRate)+"/s",
			"downloaded", common.ByteCount(stats.BytesDownload),
			"download-rate", common.ByteCount(stats.DownloadRate)+"/s",
			"webseed-trips", stats.WebseedTripCount.Load(),
			"webseed-active", stats.WebseedActiveTrips.Load(),
			"webseed-max-active", stats.WebseedMaxActiveTrips.Load(),
			"webseed-discards", stats.WebseedDiscardCount.Load(),
			"webseed-fails", stats.WebseedServerFails.Load(),
			"webseed-bytes", common.ByteCount(uint64(stats.WebseedBytesDownload.Load())))
	}
}

func (d *Downloader) newStats(prevStats AggStats) AggStats {
	torrentClient := d.torrentClient
	peers := make(map[torrent.PeerID]struct{}, 16)
	stats := prevStats
	logger := d.logger

	// Call these methods outside `lock` critical section, because they have own locks with contention.
	torrents := torrentClient.Torrents()
	connStats := torrentClient.Stats()

	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())
	stats.BytesHashed = uint64(connStats.BytesHashed.Int64())
	stats.BytesDownload = uint64(connStats.BytesReadData.Int64())
	stats.ClientWebseedBytesDownload = uint64(connStats.WebSeeds.BytesReadData.Int64())
	stats.PeerConnBytesDownload = uint64(connStats.PeerConns.BytesReadData.Int64())

	stats.BytesCompleted = 0
	stats.BytesTotal, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0
	stats.TorrentsCompleted = 0
	stats.NumTorrents = len(torrents)

	var noMetadata []string

	isDiagEnabled := diaglib.TypeOf(diaglib.SnapshoFilesList{}).Enabled()
	if isDiagEnabled {
		filesList := make([]string, 0, len(torrents))
		for _, t := range torrents {
			filesList = append(filesList, t.Name())
		}
		diaglib.Send(diaglib.SnapshoFilesList{Files: filesList})
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

		stats.BytesTotal += uint64(tLen)

		for _, peer := range peersOfThisFile {
			stats.ConnectionsTotal++
			peers[peer.PeerID] = struct{}{}
		}

		_, webseeds := getWebseedsRatesForlogs(weebseedPeersOfThisFile, torrentName, t.Complete().Bool())
		_, segmentPeers := getPeersRatesForlogs(peersOfThisFile, torrentName)

		diaglib.Send(diaglib.SegmentDownloadStatistics{
			Name:            torrentName,
			TotalBytes:      uint64(tLen),
			DownloadedBytes: uint64(bytesCompleted),
			Webseeds:        webseeds,
			Peers:           segmentPeers,
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
	calculateRate := func(counter func(*AggStats) uint64, rate func(*AggStats) *uint64) {
		*rate(&stats) = calculateRate(counter(&stats), counter(&prevStats), *rate(&prevStats), interval)
	}
	calculateRate(func(s *AggStats) uint64 { return s.BytesDownload }, func(s *AggStats) *uint64 { return &s.DownloadRate })
	calculateRate(func(s *AggStats) uint64 { return s.BytesHashed }, func(s *AggStats) *uint64 { return &s.HashRate })
	calculateRate(func(s *AggStats) uint64 { return s.BytesFlushed }, func(s *AggStats) *uint64 { return &s.FlushRate })
	calculateRate(func(s *AggStats) uint64 { return s.BytesUpload }, func(s *AggStats) *uint64 { return &s.UploadRate })
	calculateRate(func(s *AggStats) uint64 { return s.BytesCompleted }, func(s *AggStats) *uint64 { return &s.CompletionRate })
	calculateRate(func(s *AggStats) uint64 { return s.ClientWebseedBytesDownload }, func(s *AggStats) *uint64 { return &s.ClientWebseedBytesDownloadRate })
	calculateRate(func(s *AggStats) uint64 { return s.PeerConnBytesDownload }, func(s *AggStats) *uint64 { return &s.PeerConnBytesDownloadRate })

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
		// Well shit I was overflowing uint64, switching to float.
		return uint64(float64(current-previous) / interval.Seconds())
	}
	// TODO: Probably assert and find out what is wrong.
	return 0
}

// Adds segment peer fields common to Peer instances.
func setCommonPeerSegmentFields(peer *torrent.Peer, stats *torrent.PeerStats, segment *diaglib.SegmentPeer) {
	segment.DownloadRate = uint64(stats.DownloadRate)
	segment.UploadRate = uint64(stats.LastWriteUploadRate)
	segment.PiecesCount = uint64(stats.RemotePieceCount)
	segment.RemoteAddr = peer.RemoteAddr.String()
}

func getWebseedsRatesForlogs(weebseedPeersOfThisFile []*torrent.Peer, fName string, finished bool) ([]interface{}, []diaglib.SegmentPeer) {
	seeds := make([]diaglib.SegmentPeer, 0, len(weebseedPeersOfThisFile))
	webseedRates := make([]interface{}, 0, len(weebseedPeersOfThisFile)*2)
	webseedRates = append(webseedRates, "file", fName)
	for _, peer := range weebseedPeersOfThisFile {
		if peerUrl, err := webPeerUrl(peer); err == nil {
			if shortUrl, err := url.JoinPath(peerUrl.Host, peerUrl.Path); err == nil {
				stats := peer.Stats()
				if !finished {
					seed := diaglib.SegmentPeer{
						Url:         peerUrl.Host,
						TorrentName: fName,
					}
					setCommonPeerSegmentFields(peer, &stats, &seed)
					seeds = append(seeds, seed)
				}
				webseedRates = append(
					webseedRates,
					strings.TrimSuffix(shortUrl, "/"),
					common.ByteCount(uint64(stats.DownloadRate))+"/s",
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

func getPeersRatesForlogs(peersOfThisFile []*torrent.PeerConn, fName string) ([]interface{}, []diaglib.SegmentPeer) {
	peers := make([]diaglib.SegmentPeer, 0, len(peersOfThisFile))
	rates := make([]interface{}, 0, len(peersOfThisFile)*2)
	rates = append(rates, "file", fName)

	for _, peer := range peersOfThisFile {
		url := fmt.Sprintf("%v", peer.PeerClientName.Load())
		stats := peer.Stats()
		segPeer := diaglib.SegmentPeer{
			Url:         url,
			PeerId:      peer.PeerID,
			TorrentName: fName,
		}
		setCommonPeerSegmentFields(&peer.Peer, &stats, &segPeer)
		peers = append(peers, segPeer)
		rates = append(rates, url, common.ByteCount(uint64(stats.DownloadRate))+"/s")
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
		if !isStateFile && ff.Type == nil {
			return fmt.Errorf("nil ptr after parsing file: %s", name)
		}
	}

	// if we don't have the torrent file we build it if we have the .seg file
	_, err := BuildTorrentIfNeed(ctx, name, d.SnapDir(), d.torrentFS)
	if err != nil {
		return fmt.Errorf("building metainfo for new seedable file: %w", err)
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	// The above BuildTorrentIfNeed should put the metainfo in the right place for name.
	// addPreverifiedTorrent is the correct wrapper to check for existing torrents in the client.
	_, err = d.addPreverifiedTorrent(g.None[metainfo.Hash](), name)
	if err != nil {
		return fmt.Errorf("adding torrent: %w", err)
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
		err := dir.RemoveFile(miPath)
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

// RequestSnapshot Add a torrent with a known info hash. Either someone else made it, or it was on disk.
func (d *Downloader) RequestSnapshot(
	infoHash metainfo.Hash, // The infohash to use if there isn't one on disk. If there isn't one on disk then we can't proceed.
	name string,
) error {
	panicif.Zero(infoHash)
	d.lock.Lock()
	defer d.lock.Unlock()
	t, err := d.addPreverifiedTorrent(g.Some(infoHash), name)
	if err != nil {
		return err
	}
	d.addRequired(t)
	return nil
}

func (d *Downloader) addRequired(t *torrent.Torrent) {
	panicif.Nil(t)
	g.MakeMapIfNil(&d.requiredTorrents)
	g.MapInsert(d.requiredTorrents, t, struct{}{})
	d.setStartTime()
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy.
func (d *Downloader) addPreverifiedTorrent(
	infoHashHint g.Option[metainfo.Hash], // The infohash to use if there isn't one on disk. If there isn't one on disk then we can't proceed.
	name string,
) (t *torrent.Torrent, err error) {
	diskSpecOpt := d.loadSpecFromDisk(name)
	if !diskSpecOpt.Ok && !infoHashHint.Ok {
		err = fmt.Errorf("can't add torrent without infohash. name=%s", name)
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
	panicif.Zero(finalInfoHash)

	ok, err := d.shouldAddTorrent(finalInfoHash, name)
	if err != nil {
		return
	}
	if !ok {
		// Return the existing torrent to the caller. If the torrent doesn't exist we should have
		// returned with an error already.
		t, _ = d.torrentClient.Torrent(finalInfoHash)
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

	if d.cfg.VerifyTorrentData {
		d.addRequired(t)
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
	} else {
		d.setStartTime()
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
		return false, errors.New("name exists with different torrent hash")
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
		"download.rate", rateLimitString(torrent.EffectiveDownloadRateLimit(cfg.DownloadRateLimiter)),
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

// Collects Downloader states in a loggable form (the "task"). Used for logging intervals etc.
type DownloaderState string

const (
	Idle    DownloaderState = "Idle"
	Syncing DownloaderState = "Syncing"
	Seeding DownloaderState = "Seeding"
)

func (d *Downloader) state() DownloaderState {
	if !d.stats.AllTorrentsComplete() {
		return Syncing
	}
	if d.stats.NumTorrents > 0 && d.cfg.ClientConfig.Seed && d.logSeeding {
		return Seeding
	}
	return Idle
}

// Currently only called if not all torrents are complete.
func (d *Downloader) logStats() {
	d.lock.RLock()
	// This is set externally. Everything else here is only modified by the caller.
	startTime := d.startTime
	d.lock.RUnlock()
	stats := d.stats
	bytesDone := stats.BytesCompleted
	percentDone := float32(100) * (float32(bytesDone) / float32(stats.BytesTotal))
	remainingBytes := stats.BytesTotal - bytesDone

	haveAllMetadata := stats.MetadataReady == stats.NumTorrents

	var logCtx []any

	addCtx := func(ctx ...any) {
		logCtx = append(logCtx, ctx...)
	}

	if stats.PeersUnique == 0 {
		ips := d.TorrentClient().BadPeerIPs()
		if len(ips) > 0 {
			addCtx("banned peers", len(ips))
		}
	}
	state := d.state()
	switch state {
	case Syncing:
		// TODO: Include what we're syncing.
		addCtx(
			"file-metadata", fmt.Sprintf("%d/%d", stats.MetadataReady, stats.NumTorrents),
			"files", fmt.Sprintf(
				"%d/%d",
				// For now it's 1:1 files:torrents.
				stats.TorrentsCompleted,
				stats.NumTorrents,
			),
			"data", func() string {
				if haveAllMetadata {
					return fmt.Sprintf(
						"%.2f%% - %s/%s",
						percentDone,
						common.ByteCount(bytesDone),
						common.ByteCount(stats.BytesTotal),
					)
				} else {
					return common.ByteCount(bytesDone)
				}
			}(),
			// TODO: Reset on each stage.
			"time-left", calculateTime(remainingBytes, stats.CompletionRate),
			"total-time", time.Since(startTime).Truncate(time.Second).String(),
			"webseed-download", fmt.Sprintf("%s/s", common.ByteCount(stats.ClientWebseedBytesDownloadRate)),
			"peer-download", fmt.Sprintf("%s/s", common.ByteCount(stats.PeerConnBytesDownloadRate)),
			"hashing-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.HashRate)),
		)
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)

	addCtx(
		"peers", stats.PeersUnique,
		"conns", stats.ConnectionsTotal,
		"upload", fmt.Sprintf("%s/s", common.ByteCount(stats.UploadRate)),
		"alloc", common.ByteCount(m.Alloc),
		"sys", common.ByteCount(m.Sys),
	)

	log.Info(fmt.Sprintf("[%s] %s", cmp.Or(d.logPrefix, "snapshots"), state), logCtx...)

	diaglib.Send(diaglib.SnapshotDownloadStatistics{
		Downloaded:           bytesDone,
		Total:                stats.BytesTotal,
		TotalTime:            time.Since(startTime).Round(time.Second).Seconds(),
		DownloadRate:         stats.DownloadRate,
		UploadRate:           stats.UploadRate,
		Peers:                stats.PeersUnique,
		Files:                int32(stats.FilesTotal),
		Connections:          stats.ConnectionsTotal,
		Alloc:                m.Alloc,
		Sys:                  m.Sys,
		DownloadFinished:     stats.AllTorrentsComplete(),
		TorrentMetadataReady: int32(stats.MetadataReady),
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

	// This is for gopprof.
	defaultMux := http.DefaultServeMux
	defaultMux.Handle(TorrentClientStatusPath, h)
	if debugMux != nil && debugMux != defaultMux {
		debugMux.Handle(TorrentClientStatusPath, h)
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
func (s *Downloader) Delete(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// This needs to occur first to prevent it being added again, and also even if it isn't actually
	// in the Downloader right now.
	err := s.torrentFS.Delete(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		// Return the error, but try to remove everything from the client anyway.
	}
	t, ok := s.torrentsByName[name]
	if !ok {
		// Return torrent file deletion error.
		return err
	}
	// Stop seeding. Erigon will remove data-file and .torrent by self
	// But we also can delete .torrent: earlier is better (`kill -9` may come at any time)
	t.Drop()
	g.MustDelete(s.torrentsByName, name)
	// I wonder if it's an issue if this occurs before initial sync has completed.
	delete(s.requiredTorrents, t)
	// Return torrent file deletion error.
	return err
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
