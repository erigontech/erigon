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
	"bufio"
	"bytes"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	g "github.com/anacrolix/generics"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	prototypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

// Downloader - component which downloading historical files. Can use BitTorrent, or other protocols
type Downloader struct {
	torrentClient *torrent.Client

	cfg *downloadercfg.Cfg

	lock sync.RWMutex
	// Pieces having extra verification due to file size mismatches.
	piecesBeingVerified map[*torrent.Piece]struct{}
	stats               AggStats

	torrentStorage storage.ClientImplCloser

	ctx          context.Context
	stopMainLoop context.CancelFunc
	wg           sync.WaitGroup

	webseeds *WebSeeds

	logger    log.Logger
	verbosity log.Lvl

	torrentFS       *AtomicTorrentFS
	webDownloadInfo map[string]webDownloadInfo
	downloading     map[string]*downloadInfo
	downloadLimit   *rate.Limit

	stuckFileDetailedLogs bool

	logPrefix         string
	startTime         time.Time
	onTorrentComplete func(name string, hash *prototypes.H160)
}

type completedTorrentInfo struct {
	path string
	hash *prototypes.H160
}

type downloadInfo struct {
	torrent  *torrent.Torrent
	time     time.Time
	progress float32
}

type webDownloadInfo struct {
	url     *url.URL
	length  int64
	md5     string
	torrent *torrent.Torrent
}

type AggStats struct {
	MetadataReady, FilesTotal int
	NumTorrents               int
	LastMetadataUpdate        *time.Time
	PeersUnique               int32
	ConnectionsTotal          uint64

	TorrentsCompleted int

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

	lastTorrentStatus time.Time
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

// retryBackoff performs exponential backoff based on the attempt number and limited
// by the provided minimum and maximum durations.
//
// It also tries to parse Retry-After response header when a http.StatusTooManyRequests
// (HTTP Code 429) is found in the resp parameter. Hence it will return the number of
// seconds the server states it may be ready to process more requests from this client.
func calcBackoff(_min, _max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	if resp != nil {
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			if s, ok := resp.Header["Retry-After"]; ok {
				if sleep, err := strconv.ParseInt(s[0], 10, 64); err == nil {
					return time.Second * time.Duration(sleep)
				}
			}
		}
	}

	mult := math.Pow(2, float64(attemptNum)) * float64(_min)
	sleep := time.Duration(mult)
	if float64(sleep) != mult || sleep > _max {
		sleep = _max
	}

	return sleep
}

func (r *requestHandler) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	r.downloader.lock.RLock()
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
		if r := recover(); r != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
				resp.Body = nil
			}

			err = fmt.Errorf("http client panic: %s", r)
		}

		webseedActiveTrips.Add(-1)
	}()

	insertCloudflareHeaders(req)

	resp, err = r.Transport.RoundTrip(req)

	attempts := 1
	retry := true

	const minDelay = 500 * time.Millisecond
	const maxDelay = 5 * time.Second
	const maxAttempts = 10

	for err == nil && retry {
		webseedTripCount.Add(1)

		switch resp.StatusCode {
		case http.StatusOK:
			if len(req.Header.Get("Range")) > 0 {
				// the torrent lib is expecting http.StatusPartialContent so it will discard this
				// if this count is higher than 0, its likely there is a server side config issue
				// as it implies that the server is not handling range requests correctly and is just
				// returning the whole file - which the torrent lib can't handle
				//
				// TODO: We could count the bytes - probably need to take this from the req though
				// as its not clear the amount of the content which will be read.  This needs
				// further investigation - if required.
				webseedDiscardCount.Add(1)
			}

			webseedBytesDownload.Add(resp.ContentLength)
			retry = false

		// the first two statuses here have been observed from cloudflare
		// during testing.  The remainder are generally understood to be
		// retry-able http responses, calcBackoff will use the Retry-After
		// header if its available
		case http.StatusInternalServerError, http.StatusBadGateway,
			http.StatusRequestTimeout, http.StatusTooEarly,
			http.StatusTooManyRequests, http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:

			WebseedServerFails.Add(1)

			if resp.Body != nil {
				resp.Body.Close()
				resp.Body = nil
			}

			attempts++
			delayTimer := time.NewTimer(calcBackoff(minDelay, maxDelay, attempts, resp))

			select {
			case <-delayTimer.C:
				// Note this assumes the req.Body is nil
				resp, err = r.Transport.RoundTrip(req)
				webseedTripCount.Add(1)

			case <-req.Context().Done():
				err = req.Context().Err()
			}
			retry = attempts < maxAttempts

		default:
			webseedBytesDownload.Add(resp.ContentLength)
			retry = false
		}
	}

	return resp, err
}

func New(ctx context.Context, cfg *downloadercfg.Cfg, logger log.Logger, verbosity log.Lvl) (*Downloader, error) {
	// TODO: Check this!! Upstream any logic that works reliably.
	requestHandler := &requestHandler{
		Transport: http.Transport{
			Proxy:       cfg.ClientConfig.HTTPProxy,
			DialContext: cfg.ClientConfig.HTTPDialContext,
			// I think this value was observed from some webseeds. It seems reasonable to extend it
			// to other uses of HTTP from the client.
			MaxConnsPerHost: 10,
		}}

	cfg.ClientConfig.WebTransport = requestHandler

	db, err := openMdbx(ctx, cfg.Dirs.Downloader, cfg.MdbxWriteMap)
	if err != nil {
		err = fmt.Errorf("opening downloader mdbx: %w", err)
		return nil, err
	}
	defer db.Close()

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
		cfg:             cfg,
		torrentStorage:  m,
		torrentClient:   torrentClient,
		stats:           stats,
		webseeds:        NewWebSeeds(cfg.WebSeedUrls, verbosity, logger),
		logger:          logger,
		verbosity:       verbosity,
		torrentFS:       &AtomicTorrentFS{dir: cfg.Dirs.Snap},
		webDownloadInfo: map[string]webDownloadInfo{},
		downloading:     map[string]*downloadInfo{},
		logPrefix:       "",
	}
	d.webseeds.SetTorrent(d.torrentFS, cfg.DownloadTorrentFilesFromWebseed)

	requestHandler.downloader = d

	if cfg.ClientConfig.DownloadRateLimiter != nil {
		downloadLimit := cfg.ClientConfig.DownloadRateLimiter.Limit()
		d.downloadLimit = &downloadLimit
	}

	d.ctx, d.stopMainLoop = context.WithCancel(ctx)

	return d, nil
}

func (d *Downloader) MainLoopInBackground(silent bool) {
	d.spawn(func() {
		if err := d.mainLoop(silent); err != nil {
			if !errors.Is(err, context.Canceled) {
				d.logger.Warn("[snapshots]", "err", err)
			}
		}
	})
}

type seedHash struct {
	url      *url.URL
	hash     *infohash.T
	reported bool
}

func (d *Downloader) mainLoop(logSeeding bool) error {
	const logInterval = 20 * time.Second
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	var m runtime.MemStats

	for {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-logEvery.C:
			d.ReCalcStats(logInterval)
			if !d.stats.AllTorrentsComplete() {
				d.logProgress()
			}

			// Or files==0?
			if logSeeding {
				continue
			}

			stats := d.Stats()

			dbg.ReadMemStats(&m)

			if stats.AllTorrentsComplete() && stats.FilesTotal > 0 {
				d.logger.Info("[snapshots] Seeding",
					"up", common.ByteCount(stats.UploadRate)+"/s",
					"peers", stats.PeersUnique,
					"conns", stats.ConnectionsTotal,
					"files", stats.FilesTotal,
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
				)
				continue
			}

			if stats.PeersUnique == 0 {
				ips := d.TorrentClient().BadPeerIPs()
				if len(ips) > 0 {
					d.logger.Info("[snapshots] Stats", "banned", ips)
				}
			}
		}
	}
}

func (d *Downloader) getWebDownloadInfo(t *torrent.Torrent) (webDownloadInfo, []*seedHash, error) {
	d.lock.RLock()
	info, ok := d.webDownloadInfo[t.Name()]
	d.lock.RUnlock()

	if ok {
		return info, nil, nil
	}

	// todo this function does not exit on first matched webseed hash, could make unexpected results
	infos, seedHashMismatches, err := d.webseeds.getWebDownloadInfo(d.ctx, t)
	if err != nil || len(infos) == 0 {
		return webDownloadInfo{}, seedHashMismatches, fmt.Errorf("can't find download info: %w", err)
	}
	return infos[0], seedHashMismatches, nil
}

func getWebpeerTorrentInfo(ctx context.Context, downloadUrl *url.URL) (*metainfo.MetaInfo, error) {
	torrentRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadUrl.String()+".torrent", nil)

	if err != nil {
		return nil, err
	}

	torrentResponse, err := http.DefaultClient.Do(torrentRequest)

	if err != nil {
		return nil, err
	}

	defer torrentResponse.Body.Close()

	if torrentResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("can't get webpeer torrent unexpected http response: %s", torrentResponse.Status)
	}

	return metainfo.Load(torrentResponse.Body)
}

func (d *Downloader) SnapDir() string { return d.cfg.Dirs.Snap }

func (d *Downloader) allTorrentsComplete() (ret bool) {
	ret = true
	for _, t := range d.torrentClient.Torrents() {
		if !t.Complete().Bool() {
			ret = false
			continue
		}
		if !d.validateCompletedSnapshot(t) {
			ret = false
		}
	}
	// Require that we're not waiting on any piece verification.
	if len(d.piecesBeingVerified) != 0 {
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
		fp := filepath.Join(d.SnapDir(), filepath.FromSlash(f.Path()))
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
				os.Chmod(fp, 0o200)
				err = os.Truncate(fp, f.Length())
				if err != nil {
					d.logger.Crit("error truncating oversize snapshot file", "name", f.Path(), "err", err)
				}
				os.Chmod(fp, 0o400)
			}
		} else {
			d.logger.Crit("error checking snapshot file length", "name", f.Path(), "err", err)
		}
		passed = false
		d.verifyPieces(f)
	}
	return
}

// Run verification for pieces of the file that aren't already being verified.
func (d *Downloader) verifyPieces(f *torrent.File) {
	for p := range f.Pieces() {
		if g.MapContains(d.piecesBeingVerified, p) {
			continue
		}
		g.MakeMapIfNil(&d.piecesBeingVerified)
		if !g.MapInsert(d.piecesBeingVerified, p, struct{}{}).Ok {
			d.spawn(func() {
				err := p.VerifyDataContext(d.ctx)
				d.lock.Lock()
				defer d.lock.Unlock()
				g.MustDelete(d.piecesBeingVerified, p)
				if d.ctx.Err() != nil {
					return
				}
				panicif.Err(err)
			})
		}
	}
}

// Interval is how long between recalcs.
func (d *Downloader) ReCalcStats(interval time.Duration) {
	d.lock.RLock()

	torrentClient := d.torrentClient

	peers := make(map[torrent.PeerID]struct{}, 16)

	prevStats, stats := d.stats, d.stats

	logger := d.logger
	verbosity := d.verbosity

	downloading := map[string]*downloadInfo{}

	d.lock.RUnlock()

	// Call these methods outside `lock` critical section, because they have own locks with contention.
	torrents := torrentClient.Torrents()
	connStats := torrentClient.Stats()

	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())
	stats.BytesHashed = uint64(connStats.BytesHashed.Int64())
	stats.BytesDownload = uint64(connStats.BytesReadData.Int64())
	stats.BytesCompleted = 0
	// If this is an issue, might be better to do it as a torrent.Client method.
	for _, t := range torrents {
		stats.BytesCompleted += uint64(t.BytesCompleted())
	}

	if prevStats.BytesCompleted == 0 {
		prevStats.BytesCompleted = stats.BytesCompleted
	}

	lastMetadataReady := stats.MetadataReady

	stats.BytesTotal, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0
	stats.TorrentsCompleted = 0
	stats.NumTorrents = len(torrents)

	var zeroProgress []string
	var noMetadata []string

	isDiagEnabled := diagnostics.TypeOf(diagnostics.SnapshoFilesList{}).Enabled()
	if isDiagEnabled {
		filesList := make([]string, 0, len(torrents))
		for _, t := range torrents {
			filesList = append(filesList, t.Name())
		}
		diagnostics.Send(diagnostics.SnapshoFilesList{Files: filesList})
	}

	downloadedBytes := int64(0)

	for _, t := range torrents {
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
			delete(downloading, torrentName)
		} else {
			bytesCompleted = t.BytesCompleted()
		}

		progress := float32(float64(100) * (float64(bytesCompleted) / float64(tLen)))

		if info, ok := downloading[torrentName]; ok {
			if progress != info.progress {
				info.time = time.Now()
				info.progress = progress
			}
		}

		downloadedBytes += bytesCompleted
		stats.BytesTotal += uint64(tLen)

		for _, peer := range peersOfThisFile {
			stats.ConnectionsTotal++
			peers[peer.PeerID] = struct{}{}
		}

		webseedRates, webseeds := getWebseedsRatesForlogs(weebseedPeersOfThisFile, torrentName, t.Complete().Bool())
		rates, peers := getPeersRatesForlogs(peersOfThisFile, torrentName)

		// more detailed statistic: download rate of each peer (for each file)
		if !torrentComplete && progress != 0 {
			if info, ok := downloading[torrentName]; ok {
				info.time = time.Now()
				info.progress = progress
			}

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

	if lastMetadataReady != stats.MetadataReady {
		now := time.Now()
		stats.LastMetadataUpdate = &now
	}

	if len(noMetadata) > 0 {
		amount := len(noMetadata)
		if len(noMetadata) > 5 {
			noMetadata = append(noMetadata[:5], "...")
		}
		logger.Info("[snapshots] no metadata yet", "files", amount, "list", strings.Join(noMetadata, ","))
	}

	var noDownloadProgress []string

	if len(zeroProgress) > 0 {
		amount := len(zeroProgress)

		for _, file := range zeroProgress {
			if _, ok := downloading[file]; ok {
				noDownloadProgress = append(noDownloadProgress, file)
			}
		}

		if len(zeroProgress) > 5 {
			zeroProgress = append(zeroProgress[:5], "...")
		}

		logger.Info("[snapshots] no progress yet", "files", amount, "list", strings.Join(zeroProgress, ","))
	}

	if len(downloading) > 0 {
		amount := len(downloading)

		files := make([]string, 0, len(downloading))
		for file, info := range downloading {
			files = append(files, fmt.Sprintf("%s (%.0f%%)", file, info.progress))

			if dp, ok := downloading[file]; ok {
				if time.Since(dp.time) > 30*time.Minute {
					noDownloadProgress = append(noDownloadProgress, file)
				}
			}
		}
		sort.Strings(files)

		logger.Log(verbosity, "[snapshots] files progress", "files", amount, "list", strings.Join(files, ", "))
	}

	if d.stuckFileDetailedLogs && time.Since(stats.lastTorrentStatus) > 5*time.Minute {
		stats.lastTorrentStatus = time.Now()

		if len(noDownloadProgress) > 0 {
			progressStatus := getProgressStatus(torrentClient, noDownloadProgress)
			for file, status := range progressStatus {
				logger.Debug(fmt.Sprintf("[snapshots] torrent status: %s\n    %s", file,
					string(bytes.TrimRight(bytes.ReplaceAll(status, []byte("\n"), []byte("\n    ")), "\n "))))
			}
		}
	}

	stats.DownloadRate = calculateRate(stats.BytesDownload, prevStats.BytesDownload, prevStats.DownloadRate, interval)
	stats.HashRate = calculateRate(stats.BytesHashed, prevStats.BytesHashed, prevStats.HashRate, interval)
	stats.FlushRate = calculateRate(stats.BytesFlushed, prevStats.BytesFlushed, prevStats.FlushRate, interval)
	stats.UploadRate = calculateRate(stats.BytesUpload, prevStats.BytesUpload, prevStats.UploadRate, interval)
	stats.CompletionRate = calculateRate(stats.BytesCompleted, prevStats.BytesCompleted, prevStats.CompletionRate, interval)

	stats.PeersUnique = int32(len(peers))
	stats.FilesTotal = len(torrents)

	d.lock.Lock()
	d.stats = stats

	d.lock.Unlock()

	if !stats.AllTorrentsComplete() {
		log.Debug("[snapshots] downloading",
			"len", len(torrents),
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

// Calculating rate with decay in order to avoid rate spikes
func calculateRate(current, previous uint64, prevRate uint64, interval time.Duration) uint64 {
	if current > previous {
		return (current - previous) / uint64(interval.Seconds())
	}

	switch {
	case prevRate < 1000:
		return prevRate / 16
	case prevRate < 10000:
		return prevRate / 8
	case prevRate < 100000:
		return prevRate / 4
	default:
		return prevRate / 2
	}
}

type filterWriter struct {
	files     map[string][]byte
	remainder []byte
	file      string
}

func (f *filterWriter) Write(p []byte) (n int, err error) {
	written := len(p)

	p = append(f.remainder, p...)

	for len(p) > 0 {
		scanned, line, _ := bufio.ScanLines(p, false)

		if scanned > 0 {
			if len(f.file) > 0 {
				if len(line) == 0 {
					f.file = ""
				} else {
					line = append(line, '\n')
					f.files[f.file] = append(f.files[f.file], line...)
				}
			} else {
				if _, ok := f.files[string(line)]; ok {
					f.file = string(line)
				}
			}

			p = p[scanned:]
		} else {
			f.remainder = p
			p = nil
		}
	}
	return written, nil
}

func getProgressStatus(torrentClient *torrent.Client, noDownloadProgress []string) map[string][]byte {
	writer := filterWriter{
		files: map[string][]byte{},
	}

	for _, file := range noDownloadProgress {
		writer.files[file] = nil
	}

	torrentClient.WriteStatus(&writer)

	return writer.files
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

func (d *Downloader) VerifyData(ctx context.Context, whiteList []string, failFast bool) error {
	total := 0
	allTorrents := d.torrentClient.Torrents()
	toVerify := make([]*torrent.Torrent, 0, len(allTorrents))
	for _, t := range allTorrents {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.GotInfo(): //files to verify already have .torrent on disk. means must have `Info()` already
		default: // skip other files
			// TODO: This seems wrong. If we don't have the info we are missing
			// something.
			continue
		}

		exists, err := dir.FileExist(filepath.Join(d.SnapDir(), t.Name()))
		if err != nil {
			return err
		}
		if !exists {
			continue
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
		total += t.NumPieces()
	}
	d.logger.Info("[snapshots] Verify start")
	defer d.logger.Info("[snapshots] Verify done", "files", len(toVerify), "whiteList", whiteList)

	var completedPieces, completedFiles atomic.Uint64

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
						"progress", fmt.Sprintf("%.2f%%", 100*float64(completedPieces.Load())/float64(total)),
						"files", fmt.Sprintf("%d/%d", completedFiles.Load(), len(toVerify)),
						"sz_gb", downloadercfg.DefaultPieceSize*completedPieces.Load()/1024/1024/1024,
					)
				}
			}
		})
	}

	g, ctx := errgroup.WithContext(ctx)
	// We're hashing multiple torrents and the torrent library limits hash
	// concurrency per-torrent. We trigger piece verification ourselves to make
	// the load more predictable. This value would be related to whatever
	// hardware performs the hashing.
	g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
	for _, t := range toVerify {
		scheduleVerifyFile(ctx, g, t, &completedPieces)
		// Technically this requires the pieces for a given torrent to be
		// completed, but I took a shortcut after I realised. I don't think it's
		// necessary for it to be super accurate since we also have a pieces counter.
		completedFiles.Add(1)
	}

	return g.Wait()
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
	_, _, err = d.addTorrentSpec(ts)
	if err != nil {
		return fmt.Errorf("addTorrentSpec: %w", err)
	}
	return nil
}

func (d *Downloader) alreadyHaveThisName(name string) bool {
	for _, t := range d.torrentClient.Torrents() {
		if t.Info() != nil {
			if t.Name() == name {
				return true
			}
		}
	}
	return false
}

// Push this up? It's from the preverified list elsewhere. It's derived from
// snapshotsync.DownloadRequest but seems the Downloader only applies to snapshots now. TODO:
// Resolve this.
type SnapshotTuple struct {
	// Would this be variable to support BitTorrent v1/v2?
	Hash metainfo.Hash
	// Name identifying the file. This should be unique. Various extensions are
	// used on it, .torrent, .part, etc.
	Name string
}

// Loads metainfo from disk, removing it if it's invalid. Returns Some metainfo if it's valid. TODO:
// If something fishy happens here (what was an error return before), should we force a verification
// or something? If the metainfo isn't valid, the data might be wrong.
func (d *Downloader) loadSpecFromDisk(tup SnapshotTuple) (spec g.Option[*torrent.TorrentSpec]) {
	miPath := filepath.Join(d.SnapDir(), filepath.FromSlash(tup.Name)+".torrent")
	mi, err := metainfo.LoadFromFile(miPath)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	removeMetainfo := func() {
		err := os.Remove(miPath)
		if err != nil {
			d.logger.Error("error removing metainfo file", "err", err, "name", tup.Name)
		}
	}
	if err != nil {
		d.logger.Error("loading metainfo from disk", "err", err, "name", tup.Name)
		removeMetainfo()
		return
	}
	diskSpec := torrent.TorrentSpecFromMetaInfo(mi)
	if diskSpec.InfoHash != tup.Hash {
		// This is allowed if the torrent file was committed to disk. It's assumed the torrent was
		// downloaded in its entirety with a previous hash. TODO: Should we check there were actually info bytes?
		d.logger.Debug("disk metainfo hash mismatch",
			"expected", tup.Hash,
			"actual", diskSpec.InfoHash,
			"name", tup.Name)
	}
	spec.Set(diskSpec)
	return
}

func (d *Downloader) webSeedUrlStrs() iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, u := range d.cfg.WebSeedUrls {
			// WebSeed URLs must have a trailing slash if the implementation should append the file
			// name.
			if !yield(u.String() + "/") {
				return
			}
		}
	}
}

// Add a torrent with a known info hash. Either someone else made it, or it was on disk.
func (d *Downloader) addPreverifiedTorrent(
	infoHash metainfo.Hash,
	name string,
) error {
	if !IsSnapNameAllowed(name) {
		return fmt.Errorf("snap name %q is not allowed", name)
	}
	t, ok := d.torrentClient.Torrent(infoHash)
	if ok {
		// Assume the info is already obtained. Pretty sure the node won't try to add torrents until
		// previous stages are complete and this means the info is known. This can be changed.
		existingName := t.Info().Name
		if existingName != name {
			return fmt.Errorf("torrent with hash %v already exists with name %q", infoHash, existingName)
		}
		return nil
	} else if d.alreadyHaveThisName(name) {
		return fmt.Errorf("name exists with different torrent hash")
	}
	snapTup := SnapshotTuple{
		Hash: infoHash,
		Name: name,
	}
	specOpt := d.loadSpecFromDisk(snapTup)

	// TorrentSpec was created before I knew better about Go's heap... Set a default
	spec := specOpt.UnwrapOr(new(torrent.TorrentSpec))
	// This will trigger a mismatch if info bytes are known and don't match. We
	// would have already failed if the info bytes aren't known.
	spec.InfoHash = infoHash
	spec.DisplayName = cmp.Or(spec.DisplayName, name)
	spec.Sources = nil
	for s := range d.webSeedUrlStrs() {
		u := s + name + ".torrent"
		//fmt.Printf("%v: adding torrent source %q\n", snapTup, u)
		spec.Sources = append(spec.Sources, u)
	}
	t, ok, err := d.addTorrentSpec(spec)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	// We could check here if the spec was loaded from disk that the entire file was available. If
	// it wasn't we could abandon the data or clobber it with the preverified hash.

	// The following is a bit elaborate with avoiding goroutines. Maybe it's overkill.

	metainfoOnDisk := specOpt.Ok

	// TODO: If the metainfo was on disk already, do we want to ensure Info exists, and that the
	// torrent passes completion checks? The metainfo should not be stored unless it passed to begin
	// with.

	onGotInfo := func() {
		t.DownloadAll()
		// The info bytes weren't already on disk. Save them.
		if !metainfoOnDisk {
			if t.Complete().Bool() {
				d.saveNewlyCompletedMetainfo(t)
			} else {
				d.spawn(func() { d.saveMetainfoWhenComplete(t) })
			}
		}
	}

	// Aggressively minimize goroutines.
	if t.Info() != nil {
		onGotInfo()
	} else {
		d.spawn(func() {
			select {
			case <-d.ctx.Done():
				return
			case <-t.GotInfo():
			}
			onGotInfo()
		})
	}

	return nil
}

func (d *Downloader) saveMetainfoWhenComplete(t *torrent.Torrent) {
	select {
	case <-d.ctx.Done():
	case <-t.Complete().On():
		d.saveNewlyCompletedMetainfo(t)
	}
}

func (d *Downloader) saveNewlyCompletedMetainfoErr(t *torrent.Torrent) error {
	if !t.Complete().Bool() {
		return errors.New("torrent is not complete")
	}
	if !d.validateCompletedSnapshot(t) {
		return errors.New("completed torrent failed validation")
	}
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

func (d *Downloader) saveNewlyCompletedMetainfo(t *torrent.Torrent) {
	err := d.saveNewlyCompletedMetainfoErr(t)
	if err != nil {
		d.logger.Error("error saving metainfo for complete torrent", "err", err, "name", t.Name())
	}
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
	files = append(append(append(append(append(files, l1...), l2...), l3...), l4...), l5...)
	return files, nil
}

func (d *Downloader) BuildTorrentFilesIfNeed(ctx context.Context, chain string, ignore snapcfg.Preverified) error {
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

// This used to return the MDBX database. Instead that's opened separately now and should be passed
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
	//Reasons why using MMAP instead of files-API:
	// - i see "10K threads exchaused" error earlier (on `--torrent.download.slots=500` and `pd-ssd`)
	// - "sig-bus" at disk-full - may happen anyway, because DB is mmap
	// - MMAP - means less GC pressure, more zero-copy
	// - MMAP files are pre-allocated - which is not cool, but: 1. we can live with it 2. maybe can just resize MMAP in future
	// Possibly File storage needs more optimization for handles, will test. Should reduce GC
	// pressure and improve scheduler handling.
	// See also: https://github.com/erigontech/erigon/pull/10074
	// TODO: Should we force sqlite, or defer to part-file-only storage completion?
	m = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir: snapDir,
		UsePartFiles:  g.Some(true),
		Logger:        cfg.Slogger.With("storage"),
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

func (d *Downloader) SetLogPrefix(prefix string) {
	d.logPrefix = prefix
}

func (d *Downloader) logProgress() {
	var m runtime.MemStats
	prefix := d.logPrefix

	if d.logPrefix == "" {
		prefix = "snapshots"
	}

	if d.stats.AllTorrentsComplete() {
		log.Info(fmt.Sprintf("[%s] Downloading complete", prefix), "time", time.Since(d.startTime).String())
	}

	dbg.ReadMemStats(&m)

	// This should probably all just be based on the piece completion. It can't go over 100% or go backwards...
	status := "Downloading"

	bytesDone := d.stats.BytesCompleted

	percentDone := float32(100) * (float32(bytesDone) / float32(d.stats.BytesTotal))
	rate := d.stats.DownloadRate
	remainingBytes := d.stats.BytesTotal - bytesDone

	if d.stats.DownloadRate == 0 && d.stats.CompletionRate > 0 {
		status = "Verifying"
		rate = d.stats.CompletionRate
	}

	if d.stats.BytesTotal == 0 {
		percentDone = 0
	}

	timeLeft := calculateTime(remainingBytes, rate)

	if !d.stats.AllTorrentsComplete() {
		log.Info(fmt.Sprintf("[%s] %s", prefix, status),
			"files", fmt.Sprintf(
				"%d/%d",
				// For now it's 1:1 files:torrents.
				d.stats.TorrentsCompleted,
				d.stats.NumTorrents,
			),
			"data", fmt.Sprintf(
				"%.2f%% - %s/%s",
				percentDone,
				common.ByteCount(bytesDone),
				common.ByteCount(d.stats.BytesTotal),
			),
			"file-metadata", fmt.Sprintf("%d/%d", d.stats.MetadataReady, d.stats.NumTorrents),
			"time-left", timeLeft,
			"total-time", time.Since(d.startTime).Truncate(time.Second).String(),
			"download-rate", fmt.Sprintf("%s/s", common.ByteCount(d.stats.DownloadRate)),
			"completion-rate", fmt.Sprintf("%s/s", common.ByteCount(d.stats.CompletionRate)),
			"alloc", common.ByteCount(m.Alloc),
			"sys", common.ByteCount(m.Sys))
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
		return "inf"
	}
	return (time.Duration(amountLeft) * time.Second / time.Duration(rate)).Truncate(time.Second).String()
}

func (d *Downloader) Completed() bool {
	return d.allTorrentsComplete()
}

// Expose torrent client status to HTTP on the public/default serve mux used by GOPPROF=http. Only
// do this if you have a single instance.
func (d *Downloader) HandleTorrentClientStatus() {
	http.HandleFunc("/downloaderTorrentClientStatus", func(w http.ResponseWriter, r *http.Request) {
		d.torrentClient.WriteStatus(w)
	})
}

func (d *Downloader) spawn(f func()) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		f()
	}()
}
