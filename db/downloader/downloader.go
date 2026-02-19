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
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/gzip"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	g "github.com/anacrolix/generics"
	// Make Go expvars available to Prometheus for diagnostics.
	_ "github.com/anacrolix/missinggo/v2/expvar-prometheus"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/sync"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/webseed"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snaptype"
)

var debugWebseed = false

const TorrentClientStatusPath = "/downloader/torrentClientStatus"

func init() {
	_, debugWebseed = os.LookupEnv("DOWNLOADER_DEBUG_WEBSEED")
	webseed.PrintDebug = debugWebseed
}

type snapshotName = string

// Just a pair of name and known infohash.
type snapshot struct {
	InfoHash metainfo.Hash
	Name     string
}

// Snapshot pair with valid name.
type preverifiedSnapshot = snapshot

// Downloader - component which downloading historical files. Can use BitTorrent, or other protocols
type Downloader struct {
	addWebSeedOpts     []torrent.AddWebSeedsOpt
	metainfoHttpClient *http.Client
	cfg                *downloadercfg.Cfg
	logger             log.Logger
	torrentFS          *AtomicTorrentFS

	ctx  context.Context
	stop context.CancelFunc

	// The background logger only runs when there are no active downloads.
	activeDownloadRequestsLock sync.Mutex
	activeDownloadRequests     int
	zeroActiveDownloadRequests sync.Cond

	// Synchronizes state-sensitive changes to things affected by Downloader.Close.
	lock           sync.RWMutex
	torrentClient  *torrent.Client
	torrentStorage storage.ClientImplCloser
	// Tasks with lifetimes attached to Downloader.
	wg                     sync.WaitGroup
	initedBackgroundLogger bool
	torrentsByName         map[snapshotName]*torrent.Torrent
	// Torrents that were added for download. The first time a torrent is added here, the adder is
	// responsible for fetching metainfo and executing after-add handlers.
	downloads map[*torrent.Torrent]struct{}
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
}

func (me *AggStats) AllTorrentsComplete() bool {
	return me.TorrentsCompleted == me.NumTorrents
}

type requestHandler struct {
	// Separated this rather than embedded it to ensure our wrapper RoundTrip is called.
	rt http.RoundTripper
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
	insertCloudflareHeaders(req)

	resp, err = r.rt.RoundTrip(req)
	if err != nil {
		return
	}

	switch resp.StatusCode {
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
	default:
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

func MakeWebseedRoundTripper() http.RoundTripper {
	requestHandler := &requestHandler{}
	requestHandler.rt = makeTransport()
	return requestHandler
}

func New(ctx context.Context, cfg *downloadercfg.Cfg, logger log.Logger) (*Downloader, error) {
	cfg.ClientConfig.WebTransport = MakeWebseedRoundTripper()
	metainfoSourcesHttpClient := func() *http.Client {
		// Separate transport so webseed requests and metainfo fetching don't block each other.
		return &http.Client{
			Transport: MakeWebseedRoundTripper(),
		}
	}()
	cfg.ClientConfig.MetainfoSourcesClient = metainfoSourcesHttpClient

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

	d := &Downloader{
		metainfoHttpClient: metainfoSourcesHttpClient,
		cfg:                cfg,
		torrentStorage:     m,
		torrentClient:      torrentClient,
		addWebSeedOpts:     addWebSeedOpts,
		logger:             logger,
		torrentFS:          &AtomicTorrentFS{dir: cfg.Dirs.Snap},
	}

	d.zeroActiveDownloadRequests.L = &d.activeDownloadRequestsLock

	d.logConfig()

	d.ctx, d.stop = context.WithCancel(context.Background())

	return d, nil
}

// Add completed torrents from disk. It's assumed that torrents should be completed, and if they're
// partial they will be requested soon. We don't add incomplete files in case they are failures from
// previous sync attempts. These probably should be cleaned up somewhere, we'll assume the node
// knows to delete or ignore stuff that's not complete.
func (d *Downloader) AddTorrentsFromDisk(ctx context.Context) (incompleteTorrents int, err error) {
	d.log(log.LvlInfo, "Adding torrents from disk")
	var newTorrents []*torrent.Torrent
	defer func() {
		d.log(log.LvlInfo, "Finished adding torrents from disk", "new", len(newTorrents))
		go func() {
			for _, t := range newTorrents {
				d.afterAdd(t)
			}
		}()
	}()
	// The fs module should use forward slash style paths only. We need this guarantee for how we use
	// the metainfo.Info.Name field for nested snapshot names.
	err = fs.WalkDir(
		os.DirFS(d.snapDir()),
		".",
		func(path string, de fs.DirEntry, err error) error {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if err != nil {
				d.log(log.LvlWarn, "error walking snapshots dir", "path", path, "err", err)
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
				d.log(log.LvlDebug, "add torrents from disk: skipping incomplete torrent",
					"name", name)
				incompleteTorrents++
			}
			if new {
				newTorrents = append(newTorrents, t)
			}
			return nil
		},
	)
	return
}

// I haven't removed logSeeding yet because I think Alex will want it back at some point.
func (d *Downloader) InitBackgroundLogger(logSeeding bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.initedBackgroundLogger {
		return
	}
	d.initedBackgroundLogger = true
	// This go is actually intended. We hold the downloader lock but spawn also takes it. But
	// Downloader.spawn checks that the Downloader isn't closed so it's fine.
	go d.spawn(d.backgroundLogger)
}

func (d *Downloader) snapDir() string { return d.cfg.Dirs.Snap }

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

// Log the names of torrents missing metainfo. We can pass a level in to scale the urgency of the
// situation.
func (d *Downloader) logNoMetadata(lvl log.Lvl, torrents []snapshot) {
	noMetadata := make([]string, 0, len(torrents))

	for _, ps := range torrents {
		t, ok := d.torrentClient.Torrent(ps.InfoHash)
		if !ok {
			// Don't report missing metainfo, because we haven't even added it yet.
			continue
		}
		if t.Info() != nil {
			continue
		}
		noMetadata = append(noMetadata, ps.Name)
	}

	if len(noMetadata) == 0 {
		return
	}
	amount := len(noMetadata)
	if len(noMetadata) > 5 {
		noMetadata = append(noMetadata[:5], "...")
	}
	d.log(lvl, "No metadata yet", "files", amount, "list", noMetadata)
}

// We take preverifiedSnapshot because it's convenient. We want to log torrents that potentially
// aren't initialized yet.
func (d *Downloader) newStats(prevStats AggStats, torrents []snapshot) AggStats {
	peers := make(map[torrent.PeerID]struct{}, 16)
	stats := prevStats

	// Call these methods outside `lock` critical section, because they have own locks with contention.
	connStats := d.torrentClient.Stats()

	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())
	stats.BytesHashed = uint64(connStats.BytesHashed.Int64())
	stats.BytesDownload = uint64(connStats.BytesReadData.Int64())
	stats.ClientWebseedBytesDownload = uint64(connStats.WebSeeds.BytesReadData.Int64())
	stats.PeerConnBytesDownload = uint64(connStats.PeerConns.BytesReadData.Int64())

	stats.BytesCompleted = 0
	stats.BytesTotal, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0
	stats.TorrentsCompleted = 0
	stats.NumTorrents = len(torrents)

	for _, ps := range torrents {
		t, ok := d.torrentClient.Torrent(ps.InfoHash)
		if !ok {
			// Don't report missing metainfo, because we haven't even added it yet.
			continue
		}
		if t.Info() == nil {
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

	d.log(log.LvlInfo, "Verify start")
	defer d.log(log.LvlInfo, "Verify done", "files", len(toVerify), "whiteList", whiteList)

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
						d.log(log.LvlInfo, "Verify",
							"progress", fmt.Sprintf("%.2f%%", 100*float64(completedBytes.Load())/float64(totalBytes)),
							"files", fmt.Sprintf("%d/%d", completedFiles.Load(), len(toVerify)),
							"sz_gib", completedBytes.Load()>>30,
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
				return VerifyFileFailFast(ctx, t, d.snapDir(), &completedBytes)
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
				// d.ctx?
				select {
				case <-ctx.Done():
					return
				case <-logEvery.C:
					d.log(log.LvlInfo, "Verify",
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
	_, err := BuildTorrentIfNeed(ctx, name, d.snapDir(), d.torrentFS)
	if err != nil {
		return fmt.Errorf("building metainfo for new seedable file: %w", err)
	}
	// The above BuildTorrentIfNeed should put the metainfo in the right place for name.
	_, _, err = d.addCompleteTorrent(name)
	if err != nil {
		return fmt.Errorf("adding torrent: %w", err)
	}
	return nil
}

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

func (d *Downloader) webSeedUrlStrs() iter.Seq[string] {
	return slices.Values(d.cfg.WebSeedUrls)
}

// Download the provided snapshots in their entirety. No consumers should do this asynchronously.
// Logging is bound specific and bound to the lifetime of the call. Target is a name for what we're
// syncing.
func (d *Downloader) DownloadSnapshots(ctx context.Context, items []preverifiedSnapshot, target string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait, err := d.startSnapshotsDownload(ctx, items, target)
	if err != nil {
		return
	}
	return wait(ctx)
}

// Starts downloading, returns a function to wait for completion. Wait or err are returned. Wait
// must be called, even with an expired Context in order to clean up resources.
func (d *Downloader) startSnapshotsDownload(
	ctx context.Context,
	items []preverifiedSnapshot,
	target string,
) (wait func(context.Context) error, err error) {
	// Before we start logging the download, this will block/sleep the idle logger.
	d.incDownloadRequests()
	batch := downloadBatch{
		d: d,
	}
	g.MakeSliceWithCap(&batch.torrents, len(items))
	g.MakeChanWithLen(&batch.afterTasks, len(items))
	var batchCtx context.Context
	batchCtx, batch.cancel = context.WithCancelCause(d.ctx)

	batch.all.Add(1)
	go func() {
		defer batch.all.Done()
		d.logDownload(
			batchCtx,
			items,
			target,
			func() log.Lvl {
				if batch.finishedMetadataTasks.Load() {
					// If we've finished synchronously fetching webseeds directly, then missing
					// metainfos are a potential problem: We must now rely on fetching metainfo from
					// peers, which usually indicates there's an issue with webseeds.
					return log.LvlWarn
				} else {
					// We used to log this at info. There's a warning for each failed webseed fetch,
					// and then only after that's all done does it become interesting.
					return log.LvlDebug
				}
			},
		)
	}()

	defer func() {
		if err != nil {
			batch.abandon()
		}
	}()
	err = batch.addAllItems(ctx, items)
	wait = batch.wait
	return
}

func (d *Downloader) incDownloadRequests() {
	d.activeDownloadRequestsLock.Lock()
	d.activeDownloadRequests++
	d.activeDownloadRequestsLock.Unlock()
}

func (d *Downloader) decDownloadRequests() {
	d.activeDownloadRequestsLock.Lock()
	d.activeDownloadRequests--
	d.activeDownloadRequestsLock.Unlock()
	if d.activeDownloadRequests == 0 {
		d.zeroActiveDownloadRequests.Broadcast()
	}
}

// Returns all torrents, with names, because if a Torrent info isn't available, we can't just yank
// the name from there.
func (d *Downloader) allActiveSnapshots() (ret []snapshot) {
	for name, t := range d.torrentsByName {
		ret = append(ret, snapshot{
			Name:     name,
			InfoHash: t.InfoHash(),
		})
	}
	return
}

func (d *Downloader) backgroundLogger() {
	ctx := d.ctx
	for ctx.Err() == nil {
		d.activeDownloadRequestsLock.Lock()
		for d.activeDownloadRequests > 0 {
			// This goes to zero when Downloader closes, so we won't get stuck.
			d.zeroActiveDownloadRequests.Wait()
		}
		d.activeDownloadRequestsLock.Unlock()
		d.backgroundLogging(ctx)
	}
}

func (d *Downloader) backgroundLogging(ctx context.Context) {
	// Reset stats when we start background logging.
	stats := d.newStats(AggStats{}, d.allActiveSnapshots())
	interval := time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			interval = min(interval*2, 5*time.Minute)
		}
		// Ensure no download requests start while we log, it looks spammy in the logs.
		d.activeDownloadRequestsLock.Lock()
		if d.activeDownloadRequests > 0 {
			d.activeDownloadRequestsLock.Unlock()
			return
		}
		allActiveSnapshots := d.allActiveSnapshots()
		stats = d.newStats(stats, allActiveSnapshots)
		// Flexibility to add seeding and warn on unexpected behaviour in torrent client here. We
		// should probably change the status message if downloading but nobody is actively waiting
		// on sync. We can also report seeding if we see upload activity and everything is synced.
		d.logStatsInner(log.LvlDebug, stats, "Idle", nil, false)
		// There are missing torrent infos but nobody is waiting on a download request to complete.
		// Let me (anacrolix) know why this should happen.
		d.logNoMetadata(log.LvlError, allActiveSnapshots)
		d.activeDownloadRequestsLock.Unlock()
	}
}

// We take preverifiedSnapshot because it's convenient. We want to log torrents that potentially
// aren't initialized yet.
func (d *Downloader) logDownload(
	ctx context.Context,
	ts []preverifiedSnapshot,
	target string,
	getNoMetadataLvl func() log.Lvl,
) {
	startTime := time.Now()
	stats := d.newStats(AggStats{}, ts)
	interval := time.Second
	for {
		stats = d.newStats(stats, ts)
		d.logSyncStats(startTime, stats, target)
		d.logNoMetadata(getNoMetadataLvl(), ts)
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

// testStartSingleDownloadNoWait Starts a snapshot download bug doesn't care about waiting.
func (d *Downloader) testStartSingleDownload(
	ctx context.Context,
	infoHash metainfo.Hash,
	name string,
) (func(context.Context) error, error) {
	return d.startSnapshotsDownload(ctx, []preverifiedSnapshot{
		{infoHash, name},
	}, "testing")
}

// testStartSingleDownloadNoWait Starts a snapshot download bug doesn't care about waiting.
func (d *Downloader) testStartSingleDownloadNoWait(
	ctx context.Context,
	infoHash metainfo.Hash,
	name string,
) error {
	wait, err := d.startSnapshotsDownload(ctx, []preverifiedSnapshot{
		{infoHash, name},
	}, "testing")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	die := errors.New("testStartSingleDownloadNoWait")
	cancel(die)
	err = wait(ctx)
	if errors.Is(err, die) {
		err = nil
	}
	return err
}

func (d *Downloader) invalidateData(name snapshotName, infoHash metainfo.Hash) (err error) {
	_, ok := d.torrentClient.Torrent(infoHash)
	// Torrent in use, bad idea to proceed. This shouldn't happen since we should have found
	// the existing name earlier.
	panicif.True(ok)
	// Ensure the data isn't reused. We're presuming the storage in use, but we can't afford
	// to wait until another torrent is fetched, and then we mistake a non-partial file with
	// the correct size as being complete.
	err = os.Rename(d.filePathForName(name), d.filePathForName(name+".part"))
	if err != nil && errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	return
}

// Download a preverified file. That means it has a published manifest (metainfo), and a known info
// hash. Caller is responsible for flushing missing metainfos to disk when complete.
func (d *Downloader) addPreverifiedSnapshotForDownload(
	infoHash metainfo.Hash,
	name string,
) (
	t *torrent.Torrent,
	// First add of this Torrent that asked to download. The caller is responsible for adding
	// download tasks.
	firstDownloader bool,
	miOpt g.Option[*metainfo.MetaInfo],
	err error,
) {
	// Prevent anyone else from trying to add a torrent in the meanwhile, so we can do data
	// invalidation, and identify the first downloader.
	d.lock.Lock()
	defer d.lock.Unlock()
	t, ok, err := d.getExistingSnapshotTorrent(name, infoHash)
	if err != nil {
		// If the snapshot is already loaded with a different infohash (e.g. file was rebuilt by
		// background recsplit between restarts), skip the preverified download and keep using the
		// existing local torrent. This is not an error - the local file is valid.
		if existingT, nameOk := d.torrentsByName[name]; nameOk && existingT.InfoHash() != infoHash {
			d.log(log.LvlWarn, "snapshot already loaded with different infohash, keeping existing",
				"name", name,
				"existing_infohash", existingT.InfoHash().HexString(),
				"preverified_infohash", infoHash.HexString())
			t = existingT
			err = nil
			return
		}
		return
	}
	// We can invalidate data if a torrent isn't yet loaded.
	if !ok {
		miOpt, err = d.loadMatchingMetainfoOrInvalidateData(infoHash, name)
		if err != nil {
			return
		}
		var new bool
		t, new, err = d.addTorrent(name, infoHash)
		if err != nil {
			return
		}
		panicif.False(new)
	}
	g.MakeMapIfNil(&d.downloads)
	firstDownloader = !g.MapInsert(d.downloads, t, struct{}{}).Ok
	return
}

func (d *Downloader) loadMatchingMetainfoOrInvalidateData(
	infoHash metainfo.Hash,
	name string,
) (
	miOpt g.Option[*metainfo.MetaInfo],
	err error,
) {
	miOpt, err = d.maybeLoadMetainfoFromDisk(name)
	if err != nil {
		d.log(log.LvlError, "error loading metainfo from disk", "err", err, "name", name)
		err = nil
	}
	if miOpt.Ok {
		loadedIh := miOpt.Value.HashInfoBytes()
		if loadedIh == infoHash {
			return
		}
		// This is fine if we're doing initial sync. If we're not we shouldn't be here.
		d.log(log.LvlWarn, "preverified snapshot hash has changed",
			"expected", infoHash,
			"actual", loadedIh,
			"name", name)
		// Forget the metainfo we loaded, it's wrong (probably changed hash but not name...)
		miOpt.SetNone()
	} else {
		d.log(log.LvlDebug, "snapshot metainfo missing", "name", name)
	}
	err = d.invalidateData(name, infoHash)
	if err != nil {
		err = fmt.Errorf("invalidating old snapshot data: %w", err)
		return
	}
	return
}

func (d *Downloader) addedFirstDownloader(
	ctx context.Context,
	t *torrent.Torrent,
	miOpt g.Option[*metainfo.MetaInfo],
	name string,
	infoHash metainfo.Hash,
) (afterAdd func()) {
	// Try again, we would have invalidated data for changed infohashes now.
	if !miOpt.Ok {
		// Yes I mean for this error to be scoped here.
		err := d.fetchMetainfoFromWebseeds(ctx, name, infoHash)
		if err == nil {
			// Always reuse code paths to ensure no surprises later. I.e. load the metainfo again
			// through the same path that is used on a good run. No data invalidation here, at this
			// point we've added the torrent, and already invalidated if the metainfo was missing
			// the first time.
			miOpt, err = d.maybeLoadMetainfoFromDisk(name)
			if err != nil {
				// Should this error be returned instead?
				d.log(log.LvlError, "error loading metainfo from disk", "err", err, "name", name)
			}
		} else {
			d.log(log.LvlWarn, "error fetching metainfo from webseeds", "err", err, "name", name, "infohash", infoHash)
		}
	}

	if miOpt.Ok {
		// Good case: We have a metainfo with the right infohash, either just fetched from a
		// webseed, or it was cached on disk.
		err := d.applyMetainfo(miOpt.Value, t)
		if err != nil {
			d.log(log.LvlError, "error applying metainfo", "err", err, "name", name)
		}
	}

	// Defer these actions to not compete with downloading metainfos directly from webseed first.
	if t.Info() == nil {
		return func() { d.afterAddForDownloadMissingMetainfo(t, name) }
	} else {
		return func() { d.afterAddForDownloadHadMetainfo(t) }
	}

}

func (d *Downloader) addTorrentFromMetainfo(
	mi *metainfo.MetaInfo,
	name string,
	infoHash metainfo.Hash,
) (
	t *torrent.Torrent,
	new bool,
	err error,
) {
	t, new, err = d.addTorrent(name, infoHash)
	if err != nil {
		return
	}
	err = d.applyMetainfo(mi, t)
	return
}

func (d *Downloader) applyMetainfo(
	mi *metainfo.MetaInfo,
	t *torrent.Torrent,
) (
	err error,
) {
	err = t.SetInfoBytes(mi.InfoBytes)
	if err != nil {
		return
	}
	// The only field which can be verified, and isn't erigon-wide configuration (for now). If it
	// errors we also don't currently care, it's BitTorrent v2, and we're not using it.
	t.AddPieceLayers(mi.PieceLayers)
	return
}

func (d *Downloader) webseedMetainfoUrls(snapshotName string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for base := range d.webSeedUrlStrs() {
			if !yield(webseedMetainfoUrl(base, snapshotName)) {
				return
			}
		}
	}
}

func (d *Downloader) fetchMetainfoFromWebseeds(ctx context.Context, name string, ih metainfo.Hash) (err error) {
	err = errors.New("no webseed urls")
	var buf bytes.Buffer
	for base := range d.webSeedUrlStrs() {
		buf.Reset()
		var mi metainfo.MetaInfo
		var w io.Writer = &buf
		mi, err = GetMetainfoFromWebseed(ctx, base, name, d.metainfoHttpClient, w)
		if err != nil {
			d.log(log.LvlDebug, "error fetching metainfo from webseed", "err", err, "name", name, "webseed", base)
			// Whither error?
			continue
		}
		actualIh := mi.HashInfoBytes()
		if actualIh != ih {
			d.log(log.LvlWarn, "webseed infohash mismatch",
				"expected", ih,
				"actual", mi.HashInfoBytes(),
				"name", name,
				"webseed", base)
			continue
		}
		return os.WriteFile(d.metainfoFilePathForName(name), buf.Bytes(), 0o666)
	}
	err = fmt.Errorf("all webseed urls failed. last error: %w", err)
	return
}

func webseedMetainfoUrl(webseedUrlBase, snapshotName string) string {
	return webseedUrlBase + snapshotName + ".torrent"
}

func GetMetainfoFromWebseed(
	ctx context.Context,
	webseedUrlBase string,
	name string,
	httpClient *http.Client,
	w io.Writer, // Receives the serialized metainfo
) (
	mi metainfo.MetaInfo,
	err error,
) {
	url := webseedMetainfoUrl(webseedUrlBase, name)
	defer func() {
		if err != nil {
			err = fmt.Errorf("fetching from %q: %w", url, err)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected http response status code: %v", resp.StatusCode)
		return
	}
	tr := io.TeeReader(resp.Body, w)
	dec := bencode.NewDecoder(tr)
	err = dec.Decode(&mi)
	if err != nil {
		err = fmt.Errorf("decoding metainfo response body: %w", err)
		return
	}
	// Do we want dec.ReadEOF here? Answer: Yes. If we get a response that replies incorrectly with
	// valid bencoding and then more stuff we're doing the wrong thing.
	err = dec.ReadEOF()
	return
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk. This might
// be two functions now, the infoHashHint is getting a bit heavy.
func (d *Downloader) addTorrentIfComplete(
	name string,
) (t *torrent.Torrent, complete, new bool, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
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

// Add a complete snapshot file. The metainfo .torrent file should be present alongside.
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

func (d *Downloader) addCompleteTorrentFromMetainfo(
	name string,
	mi *metainfo.MetaInfo,
) (t *torrent.Torrent, new bool, err error) {
	t, new, err = d.addTorrentFromMetainfo(mi, name, mi.HashInfoBytes())
	if err != nil {
		return
	}
	if !d.cfg.VerifyTorrentData && !d.cfg.ManualDataVerification && !t.Complete().Bool() {
		d.log(log.LvlWarn, "expected completed snapshot but torrent client disagrees",
			"snapshot name", name,
			"infohash", t.InfoHash())
	}
	return
}

func (d *Downloader) saveMetainfoFromTorrent(t *torrent.Torrent) error {
	mi := t.Metainfo()
	// This checks for existence last I checked, which isn't really what we want.
	created, err := d.torrentFS.CreateWithMetaInfo(t.Info(), &mi)
	if err == nil && !created {
		err = fs.ErrExist
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

func (d *Downloader) Close() {
	d.log(log.LvlInfo, "Stopping", "files", len(d.torrentClient.Torrents()))
	d.lockingClose()
	d.wg.Wait()
	d.log(log.LvlInfo, "Stopped")
}

func (d *Downloader) lockingClose() {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.stop()
	d.log(log.LvlDebug, "Closing torrents")
	d.torrentClient.Close()
	if err := d.torrentStorage.Close(); err != nil {
		d.log(log.LvlWarn, "Error closing torrent storage", "err", err)
	}
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
func (d *Downloader) logConfig() {
	cfg := d.cfg.ClientConfig
	d.log(log.LvlInfo,
		"Torrent config",
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
		"webseed-urls", d.cfg.WebSeedUrls,
	)
}

func rateLimitString(rateLimit rate.Limit) string {
	if rateLimit == rate.Inf {
		return "∞"
	}
	// Use the same type for human-readable bytes as elsewhere.
	return datasize.ByteSize(rateLimit).String()
}

func (d *Downloader) logSyncStats(startTime time.Time, stats AggStats, target string) {
	bytesDone := stats.BytesCompleted
	remainingBytes := stats.BytesTotal - bytesDone

	var logCtx []any

	addCtx := func(ctx ...any) {
		logCtx = append(logCtx, ctx...)
	}

	addCtx(
		"time-left", calculateTime(remainingBytes, stats.CompletionRate),
		"time-elapsed", time.Since(startTime).Truncate(time.Second).String(),
	)

	d.logStatsInner(log.LvlInfo, stats, fmt.Sprintf("Syncing %v", target), logCtx, true)
}

// TODO: Determine the message from the stats, which has everything we need to know to determine a
// good message.
func (d *Downloader) logStatsInner(
	level log.Lvl,
	stats AggStats,
	msg string,
	logCtx []any,
	zeroDownload bool, // Log if download rates are zero.
) {
	bytesDone := stats.BytesCompleted
	percentDone := float32(100) * (float32(bytesDone) / float32(stats.BytesTotal))

	haveAllMetadata := stats.MetadataReady == stats.NumTorrents

	addCtx := func(ctx ...any) {
		logCtx = append(logCtx, ctx...)
	}

	if stats.PeersUnique == 0 {
		ips := d.TorrentClient().BadPeerIPs()
		if len(ips) > 0 {
			addCtx("banned peers", len(ips))
		}
	}
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
		"hashing-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.HashRate)),
	)

	if zeroDownload || (stats.ClientWebseedBytesDownloadRate != 0 || stats.PeerConnBytesDownloadRate != 0) {
		// If these were happening without active download requests it would be unusual.
		addCtx(
			"webseed-download", fmt.Sprintf("%s/s", common.ByteCount(stats.ClientWebseedBytesDownloadRate)),
			"peer-download", fmt.Sprintf("%s/s", common.ByteCount(stats.PeerConnBytesDownloadRate)),
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

	d.log(level, msg, logCtx...)
}

func calculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "∞"
	}
	return time.Duration(float64(amountLeft) / float64(rate) * float64(time.Second)).Truncate(time.Second).String()
}

// Expose torrent client status to HTTP on the public/default serve mux used by GOPPROF=http, and
// the provided "debug" mux if non-nil. Only do this if you have a single instance of a Downloader.
func (d *Downloader) HandleTorrentClientStatus(debugMux *http.ServeMux) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// WriteStatus holds the client lock until it finishes. For large torrent counts, the output
		// can be very large (90 MB).
		var buf bytes.Buffer
		gzW := gzip.NewWriter(&buf)
		d.torrentClient.WriteStatus(gzW)
		gzW.Close()
		h := r.Header.Get("Accept-Encoding")
		d.log(log.LvlDebug, "compressed torrent client status", "size", buf.Len(), "Accept-Encoding", h)
		if strings.Contains(h, "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Write(buf.Bytes())
		} else {
			gzR, err := gzip.NewReader(&buf)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			io.Copy(w, gzR)
			gzR.Close()
		}
	})

	// This is for gopprof.
	defaultMux := http.DefaultServeMux
	defaultMux.Handle(TorrentClientStatusPath, h)
	if debugMux != nil && debugMux != defaultMux {
		debugMux.Handle(TorrentClientStatusPath, h)
	}
}

func (d *Downloader) spawn(f func()) bool {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.ctx.Err() != nil {
		return false
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		f()
	}()
	return true
}

// Delete - stop seeding, remove file, remove .torrent. TODO: Double check the usage of this.
func (d *Downloader) Delete(name string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	// The ordering here no longer matters. As long as the .torrent file is deleted and torrent
	// dropped while the lock is held.
	err := d.torrentFS.Delete(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		// Return the error, but try to remove everything from the client anyway.
	}
	t, ok := d.torrentsByName[name]
	if !ok {
		// Return torrent file deletion error.
		return err
	}
	// Stop seeding. Erigon will remove data-file and .torrent by self
	// But we also can delete .torrent: earlier is better (`kill -9` may come at any time)
	t.Drop()
	g.MustDelete(d.torrentsByName, name)
	delete(d.downloads, t)
	// Return torrent file deletion error.
	return err
}

func (d *Downloader) filePathForName(name string) string {
	return filepath.Join(d.snapDir(), filepath.FromSlash(name))
}

func (d *Downloader) metainfoFilePathForName(name string) string {
	return d.filePathForName(name) + ".torrent"
}

// This does all the checks: Valid name, no overlaps on infohash and names.
func (d *Downloader) getExistingSnapshotTorrent(name string, infoHash metainfo.Hash) (t *torrent.Torrent, ok bool, err error) {
	if !IsSnapNameAllowed(name) {
		err = errors.New("invalid snapshot name")
		return
	}
	t, ok = d.torrentsByName[name]
	if ok {
		if t.InfoHash() != infoHash {
			err = fmt.Errorf("snapshot already loaded with different infohash: %v", t.InfoHash().HexString())
		}
		return
	}
	t, ok = d.torrentClient.Torrent(infoHash)
	if ok {
		// This is *really* unlikely. You would have to get the name wrong somehow, or generate a
		// SHA1 collision.
		err = fmt.Errorf("snapshot exists with a different name: %q", t.Name())
	}
	return
}

// Central function through which all must pass. Does name and infoHash validation (in
// getExistingSnapshotTorrent).
func (d *Downloader) addTorrent(name string, infoHash metainfo.Hash) (t *torrent.Torrent, new bool, err error) {
	t, ok, err := d.getExistingSnapshotTorrent(name, infoHash)
	if err != nil || ok {
		return
	}
	opts := d.makeAddTorrentOpts(infoHash)
	t, new = d.torrentClient.AddTorrentOpt(opts)
	if !new {
		return
	}
	t.SetDisplayName(name)
	g.MakeMapIfNil(&d.torrentsByName)
	g.MapMustAssignNew(d.torrentsByName, name, t)
	return
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
}

// After adding a new torrent that we want to download.
func (d *Downloader) afterAddForDownloadHadMetainfo(t *torrent.Torrent) {
	d.afterAddForDownload(t)
	t.DownloadAll()
}

func (d *Downloader) afterAddForDownloadMissingMetainfo(t *torrent.Torrent, name string) {
	// A direct request for the metainfo already failed if we're here. Now we fall back to getting
	// it from the peers or using the sources mechanism in the torrent client.
	t.AddSources(slices.Collect(d.webseedMetainfoUrls(name)))
	d.afterAddForDownload(t)
	d.spawn(func() {
		d.delayedGotInfoHandler(t)
	})
}

// This is a workaround for a minor edge case: We have no existing way to wait for infos for
// multiple Torrents, but we rarely add Torrents when the metainfo isn't available.
func (d *Downloader) delayedGotInfoHandler(t *torrent.Torrent) {
	select {
	// Make sure this handler stops if Downloader.Delete is called on it.
	case <-t.Closed():
		return
	case <-t.GotInfo():
	}
	d.log(log.LvlDebug, "got metainfo from network", "name", t.Name(), "infohash", t.InfoHash())
	// Make sure the Torrent isn't closed while we're saving the metainfo.
	d.lock.RLock()
	defer d.lock.RUnlock()
	// Don't save the metainfo in case Downloader.Delete was called. TODO: Why doesn't
	// chansync.SetOnce have a read-only form, or events.Done have a helper?
	select {
	case <-t.Closed():
		return
	default:
	}
	if err := d.saveMetainfoFromTorrent(t); err != nil {
		d.log(log.LvlWarn, "error saving delayed metainfo",
			"name", t.Name(),
			"infohash", t.InfoHash(),
			"err", err)
	}
	t.DownloadAll()
}

// After adding a new torrent that should already be completed. This is safe to call without
// Downloader.lock.
func (d *Downloader) afterAdd(t *torrent.Torrent) {
	// Should be disabled by no download rate or the disable trackers flag.
	t.AddTrackers(Trackers)
	t.AllowDataUpload()
}

func (d *Downloader) log(level log.Lvl, msg string, ctx ...any) {
	d.logger.Log(level, d.cfg.LogPrefix+msg, ctx...)
}
