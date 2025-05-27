// Copyright 2024 The Erigon Authors
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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
)

// WebSeeds - allow use HTTP-based infrastructure to support Bittorrent network
// it allows download .torrent files and data files from trusted url's (for example: S3 signed url)
type WebSeeds struct {
	lock sync.Mutex

	byFileName          snaptype.WebSeedUrls // HTTP urls of data files
	torrentUrls         snaptype.TorrentUrls // HTTP urls of .torrent files
	downloadTorrentFile bool
	seeds               []*url.URL

	logger    log.Logger
	verbosity log.Lvl

	// This doesn't belong here, it belongs in Downloader.
	torrentFiles *AtomicTorrentFS
	client       *http.Client
}

func NewWebSeeds(seeds []*url.URL, verbosity log.Lvl, logger log.Logger) *WebSeeds {
	ws := &WebSeeds{
		seeds:     seeds,
		logger:    logger,
		verbosity: verbosity,
	}

	rc := retryablehttp.NewClient()
	rc.RetryMax = 5
	rc.Logger = downloadercfg.NewRetryableHttpLogger(logger.New("app", "downloader"))
	ws.client = rc.StandardClient()
	return ws
}

func (d *WebSeeds) SetTorrent(torrentFS *AtomicTorrentFS, downloadTorrentFile bool) {
	d.downloadTorrentFile = downloadTorrentFile
	d.torrentFiles = torrentFS
}

func (d *WebSeeds) checkHasTorrents(manifestResponse snaptype.WebSeedsFromProvider, report *WebSeedCheckReport) {
	// check that for each file in the manifest, there is a corresponding .torrent file
	torrentNames := make(map[string]struct{})
	for name := range manifestResponse {
		if strings.HasSuffix(name, ".torrent") {
			torrentNames[name] = struct{}{}
		}
	}
	hasTorrents := len(torrentNames) > 0
	report.missingTorrents = make([]string, 0)
	for name := range manifestResponse {
		if !snaptype.IsSeedableExtension(name) || name == "manifest.txt" {
			continue
		}
		tname := name + ".torrent"
		if _, ok := torrentNames[tname]; !ok {
			report.missingTorrents = append(report.missingTorrents, name)
			continue
		}
		delete(torrentNames, tname)
	}

	if len(torrentNames) > 0 {
		report.danglingTorrents = make([]string, 0, len(torrentNames))
		for file := range torrentNames {
			report.danglingTorrents = append(report.danglingTorrents, file)
		}
	}
	report.torrentsOK = len(report.missingTorrents) == 0 && len(report.danglingTorrents) == 0 && hasTorrents
}

func (d *WebSeeds) VerifyManifestedBuckets(ctx context.Context, failFast bool) error {
	supErr := make([]error, 0, len(d.seeds))
	reports := make([]*WebSeedCheckReport, 0, len(d.seeds))

	for _, webSeedProviderURL := range d.seeds {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		d.logger.Debug("[snapshots.webseed] verify manifest", "url", webSeedProviderURL.String())

		rep, err := d.VerifyManifestedBucket(ctx, webSeedProviderURL)
		if err != nil {
			d.logger.Warn("[snapshots.webseed] verify manifest", "err", err)
			if failFast {
				return err
			} else {
				supErr = append(supErr, err)
			}
		}

		reports = append(reports, rep)
	}

	failed := len(supErr) > 0

	fmt.Println("-----------------------REPORTS OVERVIEW--------------------------")
	for _, rep := range reports {
		if !rep.OK() {
			failed = true
		}
		fmt.Printf("%s\n", rep.ToString(false))
	}
	if failed {
		merr := "error list:\n"
		for _, err := range supErr {
			merr += fmt.Sprintf("%s\n", err)
		}
		return fmt.Errorf("webseed: some webseeds are not OK, details above| %s", merr)
	}
	return nil
}

type WebSeedCheckReport struct {
	seed             *url.URL
	manifestExist    bool
	torrentsOK       bool
	missingTorrents  []string
	danglingTorrents []string
}

func (w *WebSeedCheckReport) sort() {
	sort.Strings(w.missingTorrents)
	sort.Strings(w.danglingTorrents)
}

func (w *WebSeedCheckReport) OK() bool {
	return w.torrentsOK && w.manifestExist
}

func (w *WebSeedCheckReport) ToString(full bool) string {
	br := "BAD"
	if w.OK() {
		br = "OK"
	}

	if !w.manifestExist {
		return fmt.Sprintf("## REPORT [%s] on %s: manifest not found\n", br, w.seed)
	}
	w.sort()
	var b strings.Builder
	b.WriteString(fmt.Sprintf("## REPORT [%s] on %s\n", br, w.seed))
	b.WriteString(fmt.Sprintf(" - manifest exist: %t\n", w.manifestExist))
	b.WriteString(fmt.Sprintf(" - missing torrents (files without torrents): %d\n", len(w.missingTorrents)))
	b.WriteString(fmt.Sprintf(" - dangling (data file not found) torrents: %d\n", len(w.danglingTorrents)))

	if !full {
		return b.String()
	}

	titles := []string{
		"Missing torrents",
		"Dangling torrents",
	}

	fnamess := [][]string{
		w.missingTorrents,
		w.danglingTorrents,
	}

	for ti, names := range fnamess {
		if len(names) == 0 {
			continue
		}
		if ti == 0 {
			b.WriteByte(10)
		}
		b.WriteString(fmt.Sprintf("# %s\n", titles[ti]))
		for _, name := range names {
			b.WriteString(fmt.Sprintf("%s\n", name))
		}
		if ti != len(fnamess)-1 {
			b.WriteByte(10)
		}
	}
	b.WriteString(fmt.Sprintf("SEED [%s] %s\n", br, w.seed.String()))
	return b.String()
}

func (d *WebSeeds) VerifyManifestedBucket(ctx context.Context, webSeedProviderURL *url.URL) (report *WebSeedCheckReport, err error) {
	report = &WebSeedCheckReport{seed: webSeedProviderURL}
	defer func() { fmt.Printf("%s\n", report.ToString(true)) }()

	manifestResponse, err := d.retrieveManifest(ctx, webSeedProviderURL)
	report.manifestExist = len(manifestResponse) != 0
	if err != nil {
		return report, err
	}

	d.checkHasTorrents(manifestResponse, report)
	return report, nil
}

func (d *WebSeeds) Discover(ctx context.Context, files []string, rootDir string) {
	listsOfFiles := d.constructListsOfFiles(ctx, d.seeds, files)
	torrentMap := d.makeTorrentUrls(listsOfFiles)
	webSeedMap := d.downloadTorrentFilesFromProviders(ctx, rootDir, torrentMap)
	d.makeWebSeedUrls(listsOfFiles, webSeedMap)
}

func (d *WebSeeds) constructListsOfFiles(
	ctx context.Context,
	httpProviders []*url.URL,
	diskProviders []string,
) []snaptype.WebSeedsFromProvider {
	log.Debug("[snapshots.webseed] providers", "http", len(httpProviders), "disk", len(diskProviders))
	listsOfFiles := make([]snaptype.WebSeedsFromProvider, 0, len(httpProviders)+len(diskProviders))

	for _, webSeedProviderURL := range httpProviders {
		if ctx.Err() != nil {
			return listsOfFiles
		}
		manifestResponse, err := d.retrieveManifest(ctx, webSeedProviderURL)
		if err != nil { // don't fail on error
			d.logger.Debug("[snapshots.webseed] get from HTTP provider", "err", err, "url", webSeedProviderURL.String())
			continue
		}
		listsOfFiles = append(listsOfFiles, manifestResponse)
	}

	// add to list files from disk
	for _, webSeedFile := range diskProviders {
		response, err := d.readWebSeedsFile(webSeedFile)
		if err != nil { // don't fail on error
			d.logger.Debug("[snapshots.webseed] get from File provider", "err", err)
			continue
		}
		listsOfFiles = append(listsOfFiles, response)
	}
	return listsOfFiles
}

func (d *WebSeeds) makeTorrentUrls(listsOfFiles []snaptype.WebSeedsFromProvider) map[url.URL]string {
	torrentMap := map[url.URL]string{}
	torrentUrls := snaptype.TorrentUrls{}
	for _, urls := range listsOfFiles {
		for name, wUrl := range urls {
			if !strings.HasSuffix(name, ".torrent") {
				continue
			}
			uri, err := url.ParseRequestURI(wUrl)
			if err != nil {
				d.logger.Debug("[snapshots] url is invalid", "url", wUrl, "err", err)
				continue
			}
			torrentUrls[name] = append(torrentUrls[name], uri)
			torrentMap[*uri] = strings.TrimSuffix(name, ".torrent")
		}
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.torrentUrls = torrentUrls
	return torrentMap
}

func (d *WebSeeds) makeWebSeedUrls(listsOfFiles []snaptype.WebSeedsFromProvider, webSeedMap map[string]struct{}) {
	webSeedUrls := snaptype.WebSeedUrls{}
	for _, urls := range listsOfFiles {
		for name, wUrl := range urls {
			if strings.HasSuffix(name, ".torrent") {
				continue
			}
			if _, ok := webSeedMap[name]; ok {
				webSeedUrls[name] = append(webSeedUrls[name], wUrl)
			}
		}
	}

	d.lock.Lock()
	d.byFileName = webSeedUrls
	d.lock.Unlock()
}

func (d *WebSeeds) TorrentUrls() snaptype.TorrentUrls {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.torrentUrls
}

func (d *WebSeeds) Len() int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return len(d.byFileName)
}

func (d *WebSeeds) ByFileName(name string) (metainfo.UrlList, bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	v, ok := d.byFileName[name]
	return v, ok
}

func (d *WebSeeds) retrieveManifest(ctx context.Context, webSeedProviderUrl *url.URL) (snaptype.WebSeedsFromProvider, error) {
	// allow: host.com/v2/manifest.txt
	u := webSeedProviderUrl.JoinPath("manifest.txt")
	{ //do HEAD request with small timeout first
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
		if err != nil {
			return nil, err
		}
		insertCloudflareHeaders(request)
		resp, err := d.client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("webseed.http: make request: %w, url=%s", err, u.String())
		}
		resp.Body.Close()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	insertCloudflareHeaders(request)

	resp, err := d.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("webseed.http: make request: %w, url=%s", err, u.String())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		d.logger.Debug("[snapshots.webseed] /manifest.txt retrieval failed, no downloads from this webseed",
			"webseed", webSeedProviderUrl.String(), "status", resp.Status)
		return nil, fmt.Errorf("webseed.http: status=%d, url=%s", resp.StatusCode, u.String())
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("webseed.http: read: %w, url=%s, ", err, u.String())
	}

	response := snaptype.WebSeedsFromProvider{}
	// Use a bytes.Scanner.
	fileNames := strings.Split(string(b), "\n")
	for fi, f := range fileNames {
		trimmed := strings.TrimSpace(f)
		switch trimmed {
		case "":
			if fi != len(fileNames)-1 {
				d.logger.Debug("[snapshots.webseed] empty line in manifest.txt", "webseed", webSeedProviderUrl.String(), "lineNum", fi)
			}
			continue
		case "manifest.txt", "node.txt":
			continue
		default:
			response[trimmed] = webSeedProviderUrl.JoinPath(trimmed).String()
		}
	}

	d.logger.Debug("[snapshots.webseed] get from HTTP provider", "manifest-len", len(response), "url", webSeedProviderUrl.String())
	return response, nil
}

func (d *WebSeeds) readWebSeedsFile(webSeedProviderPath string) (snaptype.WebSeedsFromProvider, error) {
	_, fileName := filepath.Split(webSeedProviderPath)
	data, err := os.ReadFile(webSeedProviderPath)
	if err != nil {
		return nil, fmt.Errorf("webseed.readWebSeedsFile: file=%s, %w", fileName, err)
	}
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("webseed.readWebSeedsFile: file=%s, %w", fileName, err)
	}
	d.logger.Debug("[snapshots.webseed] get from File provider", "urls", len(response), "file", fileName)
	return response, nil
}

// downloadTorrentFilesFromProviders - if they are not exist on file-system
func (d *WebSeeds) downloadTorrentFilesFromProviders(
	ctx context.Context,
	rootDir string,
	torrentMap map[url.URL]string,
) map[string]struct{} {
	// TODO: need more tests, need handle more forward-compatibility and backward-compatibility case
	//  - now, if add new type of .torrent files to S3 bucket - existing nodes will start downloading it. maybe need whitelist of file types
	//  - maybe need download new files if --snap.stop=true
	webSeedMap := map[string]struct{}{}
	var webSeedMapLock sync.RWMutex
	if !d.downloadTorrentFile {
		return webSeedMap
	}
	if len(d.TorrentUrls()) == 0 {
		return webSeedMap
	}
	var addedNew int
	e, ctx := errgroup.WithContext(ctx)
	e.SetLimit(1024)
	urlsByName := d.TorrentUrls()

	for fileName, tUrls := range urlsByName {
		name := fileName
		addedNew++
		whiteListed := strings.HasSuffix(name, ".seg.torrent") ||
			strings.HasSuffix(name, ".kv.torrent") ||
			strings.HasSuffix(name, ".v.torrent") ||
			strings.HasSuffix(name, ".ef.torrent") ||
			strings.HasSuffix(name, ".idx.torrent") ||
			strings.HasSuffix(name, ".kvei.torrent") ||
			strings.HasSuffix(name, ".bt.torrent") ||
			strings.HasSuffix(name, ".kvi.torrent") ||
			strings.HasSuffix(name, ".vi.torrent") ||
			strings.HasSuffix(name, ".txt.torrent") ||
			strings.HasSuffix(name, ".efi.torrent")
		if !whiteListed {
			_, fName := filepath.Split(name)
			d.logger.Log(d.verbosity, "[snapshots] webseed has .torrent, but we skip it because this file-type not supported yet", "name", fName)
			continue
		}
		//Erigon3 doesn't provide history of commitment (.v, .ef files), but does provide .kv:
		// - prohibit v1.0-commitment...v, v2.0-commitment...ef, etc...
		// - allow v1.0-commitment...kv
		e3blackListed := strings.Contains(name, "commitment") && (strings.HasSuffix(name, ".v.torrent") || strings.HasSuffix(name, ".ef.torrent"))
		if e3blackListed {
			_, fName := filepath.Split(name)
			d.logger.Debug("[snapshots] webseed has .torrent, but we skip it because this file-type not supported yet", "name", fName)
			continue
		}

		e.Go(func() error {
			for _, url := range tUrls {
				//validation happens inside
				_, err := d.callTorrentHttpProvider(ctx, url)
				if err != nil {
					d.logger.Debug("[snapshots] got from webseed", "name", name, "err", err, "url", url)
					continue
				}
				//don't save .torrent here - do it inside downloader.Add
				webSeedMapLock.Lock()
				webSeedMap[torrentMap[*url]] = struct{}{}
				webSeedMapLock.Unlock()
				return nil
			}
			return nil
		})
	}
	if err := e.Wait(); err != nil {
		d.logger.Debug("[snapshots] webseed discover", "err", err)
	}
	return webSeedMap
}

func (d *WebSeeds) callTorrentHttpProvider(ctx context.Context, url *url.URL) ([]byte, error) {
	if !strings.HasSuffix(url.Path, ".torrent") {
		return nil, fmt.Errorf("seems not-torrent url passed: %s", url.String())
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return nil, err
	}

	insertCloudflareHeaders(request)

	request = request.WithContext(ctx)
	resp, err := d.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("webseed.downloadTorrentFile: url=%s, %w", url.String(), err)
	}
	defer resp.Body.Close()
	//protect against too small and too big data
	if resp.ContentLength == 0 || resp.ContentLength > int64(128*datasize.MB) {
		return nil, fmt.Errorf(".torrent downloading size attack prevention: resp.ContentLength=%d, url=%s", resp.ContentLength, url.EscapedPath())
	}
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("webseed.downloadTorrentFile: read body: host=%s, url=%s, %w", url.Hostname(), url.EscapedPath(), err)
	}
	return res, nil
}
