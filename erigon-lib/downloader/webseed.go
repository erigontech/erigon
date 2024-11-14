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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
)

// WebSeeds - allow use HTTP-based infrastrucutre to support Bittorrent network
// it allows download .torrent files and data files from trusted url's (for example: S3 signed url)
type WebSeeds struct {
	lock sync.Mutex

	byFileName          snaptype.WebSeedUrls // HTTP urls of data files
	torrentUrls         snaptype.TorrentUrls // HTTP urls of .torrent files
	downloadTorrentFile bool
	torrentsWhitelist   snapcfg.Preverified
	seeds               []*url.URL

	logger    log.Logger
	verbosity log.Lvl

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

func (d *WebSeeds) getWebDownloadInfo(ctx context.Context, t *torrent.Torrent) (infos []webDownloadInfo, seedHashMismatches []*seedHash, err error) {
	torrentHash := t.InfoHash().Bytes()

	for _, webseed := range d.seeds {
		downloadUrl := webseed.JoinPath(t.Name())

		if headRequest, err := http.NewRequestWithContext(ctx, http.MethodHead, downloadUrl.String(), nil); err == nil {
			insertCloudflareHeaders(headRequest)

			headResponse, err := d.client.Do(headRequest)
			if err != nil {
				continue
			}
			headResponse.Body.Close()

			if headResponse.StatusCode != http.StatusOK {
				d.logger.Trace("[snapshots.webseed] getWebDownloadInfo: HEAD request failed",
					"webseed", webseed.String(), "name", t.Name(), "status", headResponse.Status)
				continue
			}
			if meta, err := getWebpeerTorrentInfo(ctx, downloadUrl); err == nil {
				if bytes.Equal(torrentHash, meta.HashInfoBytes().Bytes()) {
					md5tag := headResponse.Header.Get("Etag")
					if md5tag != "" {
						md5tag = strings.Trim(md5tag, "\"")
					}

					infos = append(infos, webDownloadInfo{
						url:     downloadUrl,
						length:  headResponse.ContentLength,
						md5:     md5tag,
						torrent: t,
					})
				} else {
					hash := meta.HashInfoBytes()
					seedHashMismatches = append(seedHashMismatches, &seedHash{url: webseed, hash: &hash})
				}
			}
		}
		seedHashMismatches = append(seedHashMismatches, &seedHash{url: webseed})
	}

	if len(infos) == 0 {
		d.logger.Trace("[snapshots.webseed] webseed info not found", "name", t.Name())
	}

	return infos, seedHashMismatches, nil
}

func (d *WebSeeds) SetTorrent(torrentFS *AtomicTorrentFS, whiteList snapcfg.Preverified, downloadTorrentFile bool) {
	d.downloadTorrentFile = downloadTorrentFile
	d.torrentsWhitelist = whiteList
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
		if !snaptype.IsSeedableExtension(name) {
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

func (d *WebSeeds) fetchFileEtags(ctx context.Context, manifestResponse snaptype.WebSeedsFromProvider) (tags map[string]string, invalidTags, etagFetchFailed []string, err error) {
	lock := sync.Mutex{}
	etagFetchFailed = make([]string, 0)
	tags = make(map[string]string)
	invalidTagsMap := make(map[string]string)

	eg := errgroup.Group{}
	eg.SetLimit(100)
	for name, wurl := range manifestResponse {
		name, wurl := name, wurl
		u, err := url.Parse(wurl)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("webseed.fetchFileEtags: %w", err)
		}
		eg.Go(func() error {
			md5Tag, err := d.retrieveFileEtag(ctx, u)

			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				d.logger.Debug("[snapshots.webseed] get file ETag", "err", err, "url", u.String())
				if errors.Is(err, ErrInvalidEtag) {
					invalidTagsMap[name] = md5Tag
					return nil
				}
				if errors.Is(err, ErrEtagNotFound) {
					etagFetchFailed = append(etagFetchFailed, name)
					return nil
				}
				return fmt.Errorf("webseed.fetchFileEtags: %w", err)
			}
			tags[name] = md5Tag
			return nil
		})

	}
	if err := eg.Wait(); err != nil {
		return nil, nil, nil, err
	}

	invalidTags = make([]string, 0)
	if len(invalidTagsMap) > 0 {
		for name, tag := range invalidTagsMap {
			invalidTags = append(invalidTags, fmt.Sprintf("%-50s %s", name, tag))
		}
	}
	return tags, invalidTags, etagFetchFailed, nil
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
	totalEtags       int
	invalidEtags     []string
	etagFetchFailed  []string
}

func (w *WebSeedCheckReport) sort() {
	sort.Strings(w.missingTorrents)
	sort.Strings(w.invalidEtags)
	sort.Strings(w.etagFetchFailed)
	sort.Strings(w.danglingTorrents)
}

func (w *WebSeedCheckReport) OK() bool {
	return w.torrentsOK && w.manifestExist && len(w.invalidEtags) == 0 && len(w.etagFetchFailed) == 0
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
	b.WriteString(fmt.Sprintf(" - invalid ETags format: %d/%d\n", len(w.invalidEtags), w.totalEtags))
	b.WriteString(fmt.Sprintf(" - ETag fetch failed: %d/%d\n", len(w.etagFetchFailed), w.totalEtags))

	if !full {
		return b.String()
	}

	titles := []string{
		"Missing torrents",
		"Dangling torrents",
		"Invalid ETags format",
		"ETag fetch failed",
	}

	fnamess := [][]string{
		w.missingTorrents,
		w.danglingTorrents,
		w.invalidEtags,
		w.etagFetchFailed,
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
	remoteTags, invalidTags, noTags, err := d.fetchFileEtags(ctx, manifestResponse)
	if err != nil {
		return report, err
	}

	report.invalidEtags = invalidTags
	report.etagFetchFailed = noTags
	report.totalEtags = len(remoteTags) + len(noTags)
	return report, nil
}

func (d *WebSeeds) Discover(ctx context.Context, files []string, rootDir string) {
	fmt.Println("Aaaaaa")
	listsOfFiles := d.constructListsOfFiles(ctx, d.seeds, files)
	torrentMap := d.makeTorrentUrls(listsOfFiles)
	webSeedMap := d.downloadTorrentFilesFromProviders(ctx, rootDir, torrentMap)
	d.makeWebSeedUrls(listsOfFiles, webSeedMap)
}

func (d *WebSeeds) constructListsOfFiles(ctx context.Context, httpProviders []*url.URL, diskProviders []string) []snaptype.WebSeedsFromProvider {
	log.Debug("[snapshots.webseed] providers", "http", len(httpProviders), "disk", len(diskProviders))
	listsOfFiles := make([]snaptype.WebSeedsFromProvider, 0, len(httpProviders)+len(diskProviders))

	for _, webSeedProviderURL := range httpProviders {
		select {
		case <-ctx.Done():
			return listsOfFiles
		default:
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
			if !nameWhitelisted(name, d.torrentsWhitelist) {
				fmt.Println("rej", name, wUrl)
				continue
			}
			uri, err := url.ParseRequestURI(wUrl)
			if err != nil {
				d.logger.Debug("[snapshots] url is invalid", "url", wUrl, "err", err)
				continue
			}
			fmt.Println("acc", name, wUrl)
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

var ErrInvalidEtag = errors.New("invalid etag")
var ErrEtagNotFound = errors.New("not found")

func (d *WebSeeds) retrieveFileEtag(ctx context.Context, file *url.URL) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, file.String(), nil)
	if err != nil {
		return "", err
	}

	resp, err := d.client.Do(request.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("webseed.http: %w, url=%s", err, file.String())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return "", ErrEtagNotFound
		}
		return "", fmt.Errorf("webseed.http: status code %d, url=%s", resp.StatusCode, file.String())
	}

	etag := resp.Header.Get("Etag") // file md5
	if etag == "" {
		return "", fmt.Errorf("webseed.http: file has no etag, url=%s", file.String())
	}
	return etag, nil
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
	debug.PrintStack()

	response := snaptype.WebSeedsFromProvider{}
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
func (d *WebSeeds) downloadTorrentFilesFromProviders(ctx context.Context, rootDir string, torrentMap map[url.URL]string) map[string]struct{} {
	// TODO: need more tests, need handle more forward-compatibility and backward-compatibility case
	//  - now, if add new type of .torrent files to S3 bucket - existing nodes will start downloading it. maybe need whitelist of file types
	//  - maybe need download new files if --snap.stop=true
	webSeedMap := map[string]struct{}{}
	var webSeeMapLock sync.RWMutex
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
			strings.HasSuffix(name, ".vi.torrent") ||
			strings.HasSuffix(name, ".txt.torrent") ||
			strings.HasSuffix(name, ".efi.torrent")
		fmt.Println(name)
		if !whiteListed {
			_, fName := filepath.Split(name)
			d.logger.Log(d.verbosity, "[snapshots] webseed has .torrent, but we skip it because this file-type not supported yet", "name", fName)
			continue
		}
		//Erigon3 doesn't provide history of commitment (.v, .ef files), but does provide .kv:
		// - prohibit v1-commitment...v, v2-commitment...ef, etc...
		// - allow v1-commitment...kv
		e3blackListed := strings.Contains(name, "commitment") && (strings.HasSuffix(name, ".v.torrent") || strings.HasSuffix(name, ".ef.torrent"))
		if e3blackListed {
			_, fName := filepath.Split(name)
			d.logger.Debug("[snapshots] webseed has .torrent, but we skip it because this file-type not supported yet", "name", fName)
			continue
		}

		tUrls := tUrls
		e.Go(func() error {
			for _, url := range tUrls {
				//validation happens inside
				_, err := d.callTorrentHttpProvider(ctx, url, name)
				if err != nil {
					d.logger.Debug("[snapshots] got from webseed", "name", name, "err", err, "url", url)
					continue
				}
				//don't save .torrent here - do it inside downloader.Add
				webSeeMapLock.Lock()
				webSeedMap[torrentMap[*url]] = struct{}{}
				webSeeMapLock.Unlock()
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

func (d *WebSeeds) DownloadAndSaveTorrentFile(ctx context.Context, name string) (ts *torrent.TorrentSpec, ok bool, err error) {
	urls, ok := d.ByFileName(name)
	if !ok {
		return nil, false, nil
	}
	for _, urlStr := range urls {
		urlStr += ".torrent"
		parsedUrl, err := url.Parse(urlStr)
		if err != nil {
			d.logger.Log(d.verbosity, "[snapshots] callTorrentHttpProvider parse url", "err", err)
			continue // it's ok if some HTTP provider failed - try next one
		}
		res, err := d.callTorrentHttpProvider(ctx, parsedUrl, name)
		if err != nil {
			d.logger.Debug("[snapshots] .torrent from webseed rejected", "name", name, "err", err, "url", urlStr)
			continue // it's ok if some HTTP provider failed - try next one
		}
		ts, _, err = d.torrentFiles.Create(name, res)
		return ts, ts != nil, err
	}

	return nil, false, nil
}

func (d *WebSeeds) callTorrentHttpProvider(ctx context.Context, url *url.URL, fileName string) ([]byte, error) {
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
	if err = validateTorrentBytes(fileName, res, d.torrentsWhitelist); err != nil {
		return nil, fmt.Errorf("webseed.downloadTorrentFile: host=%s, url=%s, %w", url.Hostname(), url.EscapedPath(), err)
	}
	return res, nil
}

func validateTorrentBytes(fileName string, b []byte, whitelist snapcfg.Preverified) error {
	var mi metainfo.MetaInfo
	if err := bencode.NewDecoder(bytes.NewBuffer(b)).Decode(&mi); err != nil {
		return err
	}
	torrentHash := mi.HashInfoBytes()
	// files with different names can have same hash. means need check AND name AND hash.
	if !nameAndHashWhitelisted(fileName, torrentHash.String(), whitelist) {
		return fmt.Errorf(".torrent file is not whitelisted %s", torrentHash.String())
	}
	return nil
}

func nameWhitelisted(fileName string, whitelist snapcfg.Preverified) bool {
	return whitelist.Contains(strings.TrimSuffix(fileName, ".torrent"))
}

func nameAndHashWhitelisted(fileName, fileHash string, whitelist snapcfg.Preverified) bool {
	fileName = strings.TrimSuffix(fileName, ".torrent")

	for i := 0; i < len(whitelist); i++ {
		if whitelist[i].Name == fileName && whitelist[i].Hash == fileHash {
			return true
		}
	}
	return false
}
