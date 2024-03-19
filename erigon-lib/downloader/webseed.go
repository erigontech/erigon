package downloader

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"golang.org/x/sync/errgroup"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
)

// WebSeeds - allow use HTTP-based infrastrucutre to support Bittorrent network
// it allows download .torrent files and data files from trusted url's (for example: S3 signed url)
type WebSeeds struct {
	lock sync.Mutex

	byFileName          snaptype.WebSeedUrls // HTTP urls of data files
	torrentUrls         snaptype.TorrentUrls // HTTP urls of .torrent files
	downloadTorrentFile bool
	torrentsWhitelist   snapcfg.Preverified

	seeds []*url.URL

	logger    log.Logger
	verbosity log.Lvl

	torrentFiles *TorrentFiles
}

func NewWebSeeds(seeds []*url.URL, verbosity log.Lvl, logger log.Logger) *WebSeeds {
	return &WebSeeds{
		seeds:     seeds,
		logger:    logger,
		verbosity: verbosity,
	}
}

func (d *WebSeeds) getWebDownloadInfo(ctx context.Context, t *torrent.Torrent) ([]webDownloadInfo, []*seedHash, error) {
	var seedHashMismatches []*seedHash
	var infos []webDownloadInfo
	torrentHash := t.InfoHash().Bytes()

	for _, webseed := range d.seeds {
		downloadUrl := webseed.JoinPath(t.Name())

		if headRequest, err := http.NewRequestWithContext(ctx, http.MethodHead, downloadUrl.String(), nil); err == nil {
			headResponse, err := http.DefaultClient.Do(headRequest)

			if err != nil {
				continue
			}

			headResponse.Body.Close()

			if headResponse.StatusCode != http.StatusOK {
				d.logger.Warn("[snapshots] webseed HEAD request failed", "url", downloadUrl, "status", headResponse.Status)
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

	return infos, seedHashMismatches, nil
}

func (d *WebSeeds) SetTorrent(t *TorrentFiles, whiteList snapcfg.Preverified, downloadTorrentFile bool) {
	d.downloadTorrentFile = downloadTorrentFile
	d.torrentsWhitelist = whiteList
	d.torrentFiles = t
}

func (d *WebSeeds) checkHasTorrents(manifestResponse snaptype.WebSeedsFromProvider, webSeedProviderURL *url.URL) {
	// check that for each file in the manifest, there is a corresponding .torrent file
	count := len(manifestResponse)
	for name := range manifestResponse {
		if !strings.HasSuffix(name, ".torrent") {
			continue
		}
		fn := strings.TrimSuffix(name, ".torrent")
		if !nameWhitelisted(name, d.torrentsWhitelist) {
			delete(manifestResponse, fn)
			delete(manifestResponse, name)
			continue
		}
		if _, ok := manifestResponse[fn]; !ok {
			d.logger.Warn("[snapshots.webseed] .torrent file not found for file in manifest",
				"file", name, "seed", webSeedProviderURL.String())
			continue
		}
		delete(manifestResponse, fn)
		delete(manifestResponse, name)
	}

	if len(manifestResponse) > 0 {
		d.logger.Warn("[snapshots.webseed] manifested .torrent files was not found",
			"missing", len(manifestResponse), "totalFiles", count, "seed", webSeedProviderURL.String())

		files := make([]string, 0, len(manifestResponse))
		for file := range manifestResponse {
			files = append(files, file)
		}
		sort.Strings(files)
		for _, file := range files {
			fmt.Printf("%s\n", file)
		}
		//return fmt.Errorf("missing %d .torrent files", len(files))
	}
	//return nil
}

func (d *WebSeeds) fetchFileEtags(ctx context.Context, manifestResponse snaptype.WebSeedsFromProvider) (map[string]string, error) {
	tags := make(map[string]string)
	for name, wurl := range manifestResponse {
		if strings.HasSuffix(name, ".torrent") {
			continue
		}
		//if !nameWhitelisted(name, d.torrentsWhitelist) {
		//	continue
		//}

		u, err := url.Parse(wurl)
		if err != nil {
			return nil, fmt.Errorf("webseed.fetchFileEtags: %w", err)
		}
		md5Tag, err := d.retrieveFileEtag(ctx, u)
		if err != nil {
			d.logger.Debug("[snapshots.webseed] get file ETag", "err", err, "url", u.String())
			return nil, fmt.Errorf("webseed.fetchFileEtags: %w", err)
		}
		tags[name] = md5Tag
	}
	return tags, nil
}

func (d *WebSeeds) VerifyManifestedBuckets(ctx context.Context, dirs datadir.Dirs, failFast bool) error {
	var supErr error
	for _, webSeedProviderURL := range d.seeds {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := d.VerifyManifestedBucket(ctx, dirs, webSeedProviderURL); err != nil {
			d.logger.Warn("[snapshots.webseed] verify manifest", "err", err)
			if failFast {
				return err
			} else {
				supErr = err
			}
		}
	}
	return supErr
}

func (d *WebSeeds) findLocalFileAndCheckMD5(ctx context.Context, dirs datadir.Dirs, manifestResponse snaptype.WebSeedsFromProvider) error {
	etags, err := d.fetchFileEtags(ctx, manifestResponse)
	if err != nil {
		return err
	}

	notFounds := make([]string, 0, len(etags))

	hasher := md5.New()

	walker := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}

		if strings.HasSuffix(d.Name(), ".torrent") || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		hasher.Reset()
		f, err := os.OpenFile(filepath.Join(dirs.Snap, path), os.O_RDONLY, 0640)
		if err != nil {
			return err
		}
		defer f.Close()

		webHash, found := etags[d.Name()]
		if !found {
			fmt.Printf("file %s not found in webseed\n", d.Name())
			notFounds = append(notFounds, path)
			return nil
			//return fmt.Errorf("file %s not found in webseed", d.Name())
		}

		n, err := io.Copy(hasher, f)
		if err != nil {
			return err
		}
		finf, err := d.Info()
		if err != nil {
			return err
		}
		if n != finf.Size() {
			return fmt.Errorf("hashed file size mismatch %d != expeced %d", n, finf.Size())
		}

		hashOnDisk := hex.EncodeToString(hasher.Sum(nil))
		if hashOnDisk != webHash {
			return fmt.Errorf("file %s has invalid md5 %s != %s (webseed)", d.Name(), hashOnDisk, webHash)
		}
		delete(etags, d.Name())
		return nil
	}

	for _, d := range []string{dirs.Snap /*, dirs.SnapDomain, dirs.SnapIdx, dirs.SnapHistory*/} {
		sfs := os.DirFS(d)
		if err = fs.WalkDir(sfs, ".", walker); err != nil {
			return err
		}
	}

	if len(etags) > 0 {
		fmt.Printf("Files not found on disk:\n")
		for n, e := range etags {
			fmt.Printf("%s %s\n", e, n)
		}
	}

	if len(notFounds) > 0 {
		sort.Strings(notFounds)
		fmt.Printf("Files not found on webseed:\n")
		for _, n := range notFounds {
			fmt.Printf("%s\n", n)
		}
	}

	return err
}

func (d *WebSeeds) VerifyManifestedBucket(ctx context.Context, dir datadir.Dirs, webSeedProviderURL *url.URL) error {
	manifestResponse, err := d.retrieveManifest(ctx, webSeedProviderURL)
	if err != nil {
		d.logger.Debug("[snapshots.webseed] get from HTTP provider", "err", err, "url", webSeedProviderURL.String())
		return err
	}

	d.checkHasTorrents(manifestResponse, webSeedProviderURL)

	err = d.findLocalFileAndCheckMD5(ctx, dir, manifestResponse)
	if err != nil {
		return err
	}

	//// add to list files from disk
	//for _, webSeedFile := range diskProviders {
	//	response, err := d.readWebSeedsFile(webSeedFile)
	//	if err != nil { // don't fail on error
	//		d.logger.Debug("[snapshots.webseed] get from File provider", "err", err)
	//		continue
	//	}
	//	listsOfFiles = append(listsOfFiles, response)
	//}
	//return listsOfFiles
	return nil
}

func (d *WebSeeds) Discover(ctx context.Context, files []string, rootDir string) {
	if d.torrentFiles.newDownloadsAreProhibited() {
		return
	}
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
			d.logger.Debug("[snapshots.webseed] get from HTTP provider", "err", err, "url", webSeedProviderURL.EscapedPath())
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
	defer d.lock.Unlock()
	d.byFileName = webSeedUrls
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

func (d *WebSeeds) retrieveFileEtag(ctx context.Context, file *url.URL) (string, error) {
	request, err := http.NewRequest(http.MethodHead, file.String(), nil)
	if err != nil {
		return "", err
	}

	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("webseed.http: %w, url=%s", err, file.String())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("webseed.http: status code %d, url=%s", resp.StatusCode, file.String())
	}

	etag := resp.Header.Get("Etag") // file md5
	if etag == "" {
		return "", fmt.Errorf("webseed.http: file has no etag, url=%s", file.String())
	}
	etag = strings.Trim(etag, "\"")
	if strings.Contains(etag, "-") {
		fmt.Printf("invalid etag (md5): %s %s\n", etag, file.Path)
		etag = strings.Split(etag, "-")[0]
	}

	return etag, nil
	//buuuuf, err := httputil.DumpResponse(resp, true)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(string(buuuuf))
	//return "", nil
}

func (d *WebSeeds) retrieveManifest(ctx context.Context, webSeedProviderUrl *url.URL) (snaptype.WebSeedsFromProvider, error) {
	baseUrl := webSeedProviderUrl.String()
	ref, err := url.Parse("manifest.txt")
	if err != nil {
		return nil, err
	}
	u := webSeedProviderUrl.ResolveReference(ref)
	request, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("webseed.http: %w, url=%s", err, u.String())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("webseed.http: status=%d, url=%s", resp.StatusCode, u.String())
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("webseed.http: %w, url=%s, ", err, u.String())
	}

	response := snaptype.WebSeedsFromProvider{}
	fileNames := strings.Split(string(b), "\n")
	for fi, f := range fileNames {
		if strings.TrimSpace(f) == "" {
			fmt.Printf("empty line in manifest %q at line %d\n", f, fi)
			continue
		}

		response[f], err = url.JoinPath(baseUrl, f)
		if err != nil {
			return nil, err
		}
	}
	d.logger.Debug("[snapshots.webseed] get from HTTP provider", "urls", len(response), "url", webSeedProviderUrl.EscapedPath())
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
		if !strings.HasSuffix(name, ".seg.torrent") {
			_, fName := filepath.Split(name)
			d.logger.Log(d.verbosity, "[snapshots] webseed has .torrent, but we skip it because this file-type not supported yet", "name", fName)
			continue
		}

		tUrls := tUrls
		e.Go(func() error {
			for _, url := range tUrls {
				res, err := d.callTorrentHttpProvider(ctx, url, name)
				if err != nil {
					d.logger.Log(d.verbosity, "[snapshots] got from webseed", "name", name, "err", err, "url", url)
					continue
				}
				if !d.torrentFiles.Exists(name) {
					if err := d.torrentFiles.Create(name, res); err != nil {
						d.logger.Log(d.verbosity, "[snapshots] .torrent from webseed rejected", "name", name, "err", err, "url", url)
						continue
					}
				}
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

func (d *WebSeeds) callTorrentHttpProvider(ctx context.Context, url *url.URL, fileName string) ([]byte, error) {
	request, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("webseed.downloadTorrentFile: host=%s, url=%s, %w", url.Hostname(), url.EscapedPath(), err)
	}
	defer resp.Body.Close()
	//protect against too small and too big data
	if resp.ContentLength == 0 || resp.ContentLength > int64(128*datasize.MB) {
		return nil, nil
	}
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("webseed.downloadTorrentFile: host=%s, url=%s, %w", url.Hostname(), url.EscapedPath(), err)
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
		return fmt.Errorf(".torrent file is not whitelisted")
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
