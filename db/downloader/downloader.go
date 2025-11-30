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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"maps"
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

	"github.com/anacrolix/torrent/bencode"
	"github.com/puzpuzpuz/xsync/v4"

	"github.com/c2h5oh/datasize"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/anacrolix/chansync"
	g "github.com/anacrolix/generics"
	// Make Go expvars available to Prometheus for diagnostics.
	_ "github.com/anacrolix/missinggo/v2/expvar-prometheus"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/webseed"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
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

	lock sync.RWMutex
	// Files having extra verification due to file size mismatches. Use atomics for this map since
	// locking requirements are mixed, and it's not coupled to anything else.
	filesBeingVerified    *xsync.Map[*torrent.File, struct{}]
	verificationOccurring chansync.Flag
	// The "name" or file path of a torrent is used throughout as a unique identifier for the
	// torrent. Make it explicit rather than fold it into DisplayName or guess at the name at
	// various points. This might change if multi-file torrents are used.
	torrentsByName map[string]*torrent.Torrent
	stats          AggStats
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
	// Separated this rather than embedded it to ensure our wrapper RoundTrip is called.
	rt         http.RoundTripper
	downloader *Downloader
}

var cloudflareHeaders = http.Header{
	"lsjdjwcush6jbnjj3jnjscoscisoc5s": []string{"I%OSJDNFKE783DDHHJD873EFSIVNI7384R78SSJBJBCCJBC32JABBJCBJK45"},
}

func insertCloudflareHeaders(req *http.Request) {
	// Note this is clobbering the headers.
	maps.Copy(req.Header, cloudflareHeaders)
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
	resp, err = r.rt.RoundTrip(req)
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
		requestHandler.rt = makeTransport()
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

	for value := range cfg.SeparateWebseedDownloadRateLimit.Iter {
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

	return d, nil
}

// Add completed torrents from disk. It's assumed that torrents should be completed, and if they're
// partial they will be requested soon. We don't add incomplete files in case they are failures from
// previous sync attempts. These probably should be cleaned up somewhere, we'll assume the node
// knows to delete or ignore stuff that's not complete.
func (d *Downloader) AddTorrentsFromDisk(ctx context.Context) (err error) {
	var incompleteTorrents int
	defer func() {
		if incompleteTorrents != 0 {
			d.logger.Warn("add torrents from disk: skipped incomplete torrents",
				"count", incompleteTorrents)
		}
	}()
	var newTorrents []*torrent.Torrent
	// Can we lock inside the walk instead?
	d.lock.Lock()
	// Does WalkDir do path or filepath?
	err = fs.WalkDir(
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
			t, complete, new, err := d.addTorrentIfComplete(name)
			if err != nil {
				err = fmt.Errorf("adding torrent for %v: %w", path, err)
				return err
			}
			if !complete {
				d.logger.Debug("add torrents from disk: skipping incomplete torrent",
					"name", name)
				incompleteTorrents++
			}
			if new {
				newTorrents = append(newTorrents, t)
			}
			return nil
		},
	)
	d.lock.Unlock()
	if err != nil {
		return
	}
	for _, t := range newTorrents {
		d.afterAdd(t)
	}
	return nil
}

// This should only be called once...?
func (d *Downloader) MainLoopInBackground(logSeeding bool) {
	d.spawn(func() {
		// TODO: Idle logger here.
	})
}

func (d *Downloader) SnapDir() string { return d.cfg.Dirs.Snap }

// Might want to maintain a list of incomplete torrents to optimize this. Will be lazy for now.
func (d *Downloader) allTorrentsComplete() bool {
	for _, t := range d.torrentClient.Torrents() {
		if !t.Complete().Bool() {
			return false
		}
	}
	return true
}

// Check snapshot data looks right.
func (d *Downloader) snapshotDataLooksComplete(info *metainfo.Info) bool {
	for f := range info.UpvertedFilesIter() {
		pathParts := append([]string{info.BestName()}, f.BestPath()...)
		slashPath := path.Join(pathParts...)
		expectedLength := f.Length
		// Technically this works, we're preemptively handling multifile torrents here.
		fp := d.filePathForName(slashPath)
		fi, err := os.Stat(fp)
		if err != nil {
			return false
		}
		if fi.Size() != expectedLength {
			return false
		}
	}
	return true
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

// We take PreverifiedSnapshot because it's convenient. We want to log torrents that potentially
// aren't initialized yet.
func (d *Downloader) newStats(prevStats AggStats, torrents []PreverifiedSnapshot) AggStats {
	torrentClient := d.torrentClient
	peers := make(map[torrent.PeerID]struct{}, 16)
	stats := prevStats
	logger := d.logger

	// Call these methods outside `lock` critical section, because they have own locks with contention.
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

	for _, ps := range torrents {
		t, ok := d.torrentClient.Torrent(ps.InfoHash)
		if !ok {
			// Don't report missing metainfo, because we haven't even added it yet.
			continue
		}
		if t.Info() == nil {
			noMetadata = append(noMetadata, ps.Name)
			continue
		}

		stats.BytesCompleted += uint64(t.BytesCompleted())
		torrentComplete := t.Complete().Bool()
		stats.MetadataReady++

		// call methods once - to reduce internal mutex contention
		peersOfThisFile := t.PeerConns()

		tLen := t.Length()

		if torrentComplete {
			stats.TorrentsCompleted++
		}

		stats.BytesTotal += uint64(tLen)

		for _, peer := range peersOfThisFile {
			stats.ConnectionsTotal++
			peers[peer.PeerID] = struct{}{}
		}
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
	failFast bool,
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

	if failFast {
		var completedBytes atomic.Uint64
		g, ctx := errgroup.WithContext(ctx)

		{
			logEvery := time.NewTicker(10 * time.Second)
			defer logEvery.Stop()
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-logEvery.C:
						d.logger.Info("[snapshots] Verify",
							"progress", fmt.Sprintf("%.2f%%", 100*float64(completedBytes.Load())/float64(totalBytes)),
							"files", fmt.Sprintf("%d/%d", completedFiles.Load(), len(toVerify)),
							"sz_gb", downloadercfg.DefaultPieceSize*completedBytes.Load()/1024/1024/1024,
						)
					}
				}
			}()
		}

		// torrent lib internally limiting amount of hashers per file
		// set limit here just to make load predictable, not to control Disk/CPU consumption
		g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
		for _, t := range toVerify {
			g.Go(func() error {
				defer completedFiles.Add(1)
				return VerifyFileFailFast(ctx, t, d.SnapDir(), &completedBytes)
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}
		return nil
	}

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
	_, _, err = d.addCompleteTorrent(name)
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
func (d *Downloader) loadSpecFromDiskErr(name string) (spec g.Option[*torrent.TorrentSpec], err error) {
	miPath := d.filePathForName(name) + ".torrent"
	mi, err := metainfo.LoadFromFile(miPath)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		return
	}
	// Defer checking the metainfo is correct to the caller.
	spec.Set(torrent.TorrentSpecFromMetaInfo(mi))
	return
}

// Loads metainfo from disk, removing it if it's invalid. Returns Some metainfo if it's valid. Logs
// errors.
func (d *Downloader) loadMetainfoFromDisk(name string) (mi *metainfo.MetaInfo, err error) {
	miPath := d.metainfoFilePathForName(name)
	return metainfo.LoadFromFile(miPath)
}

// Loads metainfo from disk, removing it if it's invalid. Returns Some metainfo if it's valid. Logs
// errors.
func (d *Downloader) maybeLoadMetainfoFromDisk(name string) (miOpt g.Option[*metainfo.MetaInfo], err error) {
	miPath := d.metainfoFilePathForName(name)
	mi, err := metainfo.LoadFromFile(miPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		return
	}
	miOpt.Set(mi)
	return
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

type PreverifiedSnapshot struct {
	Name     string
	InfoHash metainfo.Hash
}

// Download the provided snapshots in their entirety. No consumers should do this asynchronously.
// Now we can log properly. Target is a name for what we're syncing.
func (d *Downloader) DownloadSnapshots(ctx context.Context, items []PreverifiedSnapshot, target string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	torrents := make([]*torrent.Torrent, 0, len(items))
	// This can be moved earlier to include the initialization.
	go d.logDownload(ctx, items, target)
	for _, it := range items {
		t, err := d.addPreverifiedTorrent(it.InfoHash, it.Name)
		if err != nil {
			return fmt.Errorf("adding %q: %w", it.Name, err)
		}
		torrents = append(torrents, t)
	}
	for _, t := range torrents {
		select {
		case <-t.Complete().On():
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

// We take PreverifiedSnapshot because it's convenient. We want to log torrents that potentially
// aren't initialized yet.
func (d *Downloader) logDownload(ctx context.Context, ts []PreverifiedSnapshot, target string) {
	startTime := time.Now()
	stats := d.newStats(AggStats{}, ts)
	interval := time.Second
	for {
		stats = d.newStats(stats, ts)
		d.logStats(startTime, stats, Syncing, target)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
		interval = min(interval*2, 15*time.Second)
	}
}

// addPreverifiedUnlocked Requests that a torrent with a known infohash be downloaded in entirety.
func (d *Downloader) addPreverifiedUnlocked(
	infoHash metainfo.Hash,
	name string,
) error {
	panicif.Zero(infoHash)
	d.lock.Lock()
	defer d.lock.Unlock()
	_, err := d.addPreverifiedTorrent(infoHash, name)
	if err != nil {
		return err
	}
	return nil
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy. Caller is responsible for flushing
// missing metainfos to disk when complete.
func (d *Downloader) addPreverifiedTorrent(
	infoHash metainfo.Hash, // The infohash to use if there isn't one on disk. If there isn't one on disk then we can't proceed.
	name string,
) (t *torrent.Torrent, err error) {
	panicif.False(IsSnapNameAllowed(name))
	t, ok := d.torrentsByName[name]
	if ok {
		if t.InfoHash() != infoHash {
			err = fmt.Errorf("snapshot already loaded with different infohash %v", t.InfoHash().HexString())
		}
		return
	}
	miOpt, err := d.maybeLoadMetainfoFromDisk(name)
	if miOpt.Ok {
		loadedIh := miOpt.Value.HashInfoBytes()
		if loadedIh != infoHash {
			// This is fine if we're doing initial sync. If we're not we shouldn't be here.
			d.logger.Warn("preverified snapshot hash has changed",
				"expected", infoHash,
				"actual", loadedIh,
				"name", name)
			_, ok := d.torrentClient.Torrent(infoHash)
			panicif.True(ok) // TODO
			// Ensure the data isn't reused automatically. TODO: Check this only occurs if the size
			// changes? Enough to always part it, since if the hash changes, the torrent client
			// should definitely be checking it all again anyway?
			err := os.Rename(d.filePathForName(name), d.filePathForName(name+".part"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				panic(err) // TODO
			}
			miOpt.SetNone()
		}
	}
	if !miOpt.Ok {
		err := d.fetchMetainfoFromWebseeds(name, infoHash)
		if err == nil {
			// Always reuse code paths to ensure no surprises later.
			miOpt, err = d.maybeLoadMetainfoFromDisk(name)
			if err != nil {
				d.logger.Error("error loading metainfo from disk", "err", err, "name", name)
			}
		} else {
			d.logger.Info("error fetching metainfo from webseeds", "err", err, "name", name, "infohash", infoHash)
		}
	}

	if miOpt.Ok {
		t, err = d.addNewTorrentFromMetainfo(miOpt.Value, name, infoHash)
	} else {
		t = d.addNewTorrent(name, infoHash)
		// Old behaviour, let the torrent client try to fetch the metainfo too. If the synchronous
		// request for the metainfo already failed for a good reason, then we should expect to get
		// it from the peers at this point instead.
		t.AddSources(slices.Collect(d.webseedMetainfoUrls(name)))
	}
	d.afterAddForDownload(t)
	return
}

func (d *Downloader) addNewTorrentFromMetainfo(
	mi *metainfo.MetaInfo,
	name string,
	infoHash metainfo.Hash,
) (
	t *torrent.Torrent,
	err error,
) {
	t = d.addNewTorrent(name, infoHash)
	err = t.SetInfoBytes(mi.InfoBytes)
	if err != nil {
		return
	}
	// The only field which can be verified, and isn't erigon-wide configuration (for now). If it
	// errors we also don't currently care, it's BitTorrent v2 and we're not using it.
	t.AddPieceLayers(mi.PieceLayers)
	return
}

func (d *Downloader) sourcesForName(name string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for s := range d.webSeedUrlStrs() {
			u := s + name + ".torrent"
			if !yield(u) {
				return
			}
		}
	}
}

func (d *Downloader) webseedMetainfoUrls(snapshotName string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for base := range d.webSeedUrlStrs() {
			if !yield(d.webseedMetainfoUrl(base, snapshotName)) {
				return
			}
		}
	}
}

func (d *Downloader) fetchMetainfoFromWebseeds(name string, ih metainfo.Hash) (err error) {
	err = errors.New("no webseed urls")
	var buf bytes.Buffer
	for base := range d.webSeedUrlStrs() {
		buf.Reset()
		var mi metainfo.MetaInfo
		mi, err = d.fetchMetainfoFromWebseed(name, base, &buf)
		if err != nil {
			d.logger.Debug("error fetching metainfo from webseed", "err", err, "name", name, "webseed", base)
			// Whither error?
			continue
		}
		actualIh := mi.HashInfoBytes()
		if actualIh != ih {
			d.logger.Warn("webseed infohash mismatch",
				"expected", ih,
				"actual", mi.HashInfoBytes(),
				"name", name,
				"webseed", base)
			continue
		}
		return os.WriteFile(d.metainfoFilePathForName(name), buf.Bytes(), 0o666)
	}
	return
}

func (d *Downloader) webseedMetainfoUrl(webseedUrlBase, snapshotName string) string {
	return webseedUrlBase + snapshotName + ".torrent"
}

// TODO: Maybe don't clobber the metainfo file if this is run in parallel. Remove before merge PR
// if that's not the route taken.
func (d *Downloader) fetchMetainfoFromWebseed(name string, webseedUrlBase string, w io.Writer) (
	mi metainfo.MetaInfo,
	err error,
) {
	// TODO: Use context request
	resp, err := http.Get(d.webseedMetainfoUrl(webseedUrlBase, name))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	tr := io.TeeReader(resp.Body, w)
	dec := bencode.NewDecoder(tr)
	err = dec.Decode(&mi)
	if err != nil {
		err = fmt.Errorf("decoding metainfo response body: %w", err)
		return
	}
	// Do we want dec.ReadEOF here?
	return
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy.
func (d *Downloader) addTorrentIfComplete(
	name string,
) (t *torrent.Torrent, complete, new bool, err error) {
	mi, err := d.loadMetainfoFromDisk(name)
	if err != nil {
		err = fmt.Errorf("loading metainfo from disk: %w", err)
		return
	}
	info, err := mi.UnmarshalInfo()
	if err != nil {
		err = fmt.Errorf("unmarshalling info from metainfo: %w", err)
		return
	}
	if !d.snapshotDataLooksComplete(&info) {
		err = nil
		return
	}
	t, new, err = d.addCompleteTorrentFromMetainfo(name, mi)
	complete = err == nil
	return
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy.
func (d *Downloader) addCompleteTorrent(
	name string,
) (t *torrent.Torrent, new bool, err error) {
	mi, err := d.loadMetainfoFromDisk(name)
	if err != nil {
		err = fmt.Errorf("loading metainfo from disk: %w", err)
		return
	}
	return d.addCompleteTorrentFromMetainfo(name, mi)
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy.
func (d *Downloader) addCompleteTorrentFromMetainfo(
	name string,
	mi *metainfo.MetaInfo,
) (t *torrent.Torrent, new bool, err error) {
	t, ok := d.torrentsByName[name]
	if ok {
		expectedInfoHash := mi.HashInfoBytes()
		// TODO: Just log this
		panicif.NotEq(t.InfoHash(), expectedInfoHash)
	} else {
		new = true
		t, err = d.addNewTorrentFromMetainfo(mi, name, mi.HashInfoBytes())
		if err != nil {
			return
		}
	}
	// TODO: Handle manual verification and ignore piece completion flags.
	panicif.False(t.Complete().Bool())
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
	dbCfg := mdbx.New(dbcfg.DownloaderDB, log.New()).
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

// Currently only called if not all torrents are complete.
func (d *Downloader) logStats(startTime time.Time, stats AggStats, state DownloaderState, target string) {
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

	log.Info(fmt.Sprintf("[%s] %s %s", cmp.Or(d.logPrefix, "snapshots"), state, target), logCtx...)
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

// Delete - stop seeding, remove file, remove .torrent. TODO: Double check the usage of this.
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
	// Return torrent file deletion error.
	return err
}

func (d *Downloader) filePathForName(name string) string {
	return filepath.Join(d.SnapDir(), filepath.FromSlash(name))
}

func (d *Downloader) metainfoFilePathForName(name string) string {
	return d.filePathForName(name) + ".torrent"
}

func (d *Downloader) addNewTorrent(name string, infoHash metainfo.Hash) *torrent.Torrent {
	opts := d.makeAddTorrentOpts(infoHash)
	t, new := d.torrentClient.AddTorrentOpt(opts)
	panicif.False(new)
	t.SetDisplayName(name)
	g.MakeMapIfNil(&d.torrentsByName)
	g.MapMustAssignNew(d.torrentsByName, name, t)
	return t
}

func (d *Downloader) makeAddTorrentOpts(
	infoHash metainfo.Hash,
) (ts torrent.AddTorrentOpts) {
	ts.InfoHash = infoHash
	ts.ChunkSize = downloadercfg.NetworkChunkSize
	ts.DisallowDataDownload = true
	ts.DisallowDataUpload = true
	// I wonder how this should be handled for AddNewSeedableFile. What if there's bad piece
	// completion data? We might want to clobber any piece completion and force the client to accept
	// what we provide, assuming we trust our own metainfo generation more.
	ts.IgnoreUnverifiedPieceCompletion = d.cfg.VerifyTorrentData
	ts.DisableInitialPieceCheck = d.cfg.ManualDataVerification
	return
}

// After adding a new torrent that we want to download.
func (d *Downloader) afterAddForDownload(t *torrent.Torrent) {
	d.afterAdd(t)
	// add webseed first - otherwise opts will be ignored
	t.AddWebSeeds(d.cfg.WebSeedUrls, d.addWebSeedOpts...)
	t.AllowDataDownload()
	t.DownloadAll()
}

// After adding a new torrent that should already be completed.
func (d *Downloader) afterAdd(t *torrent.Torrent) {
	// Should be disabled by no download rate or the disable trackers flag.
	t.AddTrackers(Trackers)
	t.AllowDataUpload()
}
