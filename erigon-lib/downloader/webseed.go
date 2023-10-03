package downloader

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"
)

// WebSeeds - allow use HTTP-based infrastrucutre to support Bittorrent network
// it allows download .torrent files and data files from trusted url's (for example: S3 signed url)
type WebSeeds struct {
	lock        sync.Mutex
	byFileName  snaptype.WebSeedUrls // HTTP urls of data files
	torrentUrls snaptype.TorrentUrls // HTTP urls of .torrent files

	logger log.Logger
}

func (d *WebSeeds) Discover(ctx context.Context, urls []*url.URL, files []string, rootDir string) {
	d.downloadWebseedTomlFromProviders(ctx, urls, files)
	d.downloadTorrentFilesFromProviders(ctx, rootDir)
}

func (d *WebSeeds) downloadWebseedTomlFromProviders(ctx context.Context, urls []*url.URL, files []string) {
	list := make([]snaptype.WebSeedsFromProvider, 0, len(urls)+len(files))
	for _, webSeedProviderURL := range urls {
		select {
		case <-ctx.Done():
			break
		default:
		}
		response, err := d.callWebSeedsProvider(ctx, webSeedProviderURL)
		if err != nil { // don't fail on error
			d.logger.Warn("[downloader] callWebSeedsProvider", "err", err, "url", webSeedProviderURL.EscapedPath())
			continue
		}
		list = append(list, response)
	}
	for _, webSeedFile := range files {
		response, err := d.readWebSeedsFile(webSeedFile)
		if err != nil { // don't fail on error
			_, fileName := filepath.Split(webSeedFile)
			d.logger.Warn("[downloader] readWebSeedsFile", "err", err, "file", fileName)
			continue
		}
		list = append(list, response)
	}

	webSeedUrls, torrentUrls := snaptype.NewWebSeedUrls(list)
	d.lock.Lock()
	defer d.lock.Unlock()
	d.byFileName = webSeedUrls
	d.torrentUrls = torrentUrls
}

// downloadTorrentFilesFromProviders - if they are not exist on file-system
func (d *WebSeeds) downloadTorrentFilesFromProviders(ctx context.Context, rootDir string) {
	if len(d.TorrentUrls()) == 0 {
		return
	}
	e, ctx := errgroup.WithContext(ctx)
	for name, tUrls := range d.TorrentUrls() {
		tPath := filepath.Join(rootDir, name)
		if dir.FileExist(tPath) {
			continue
		}
		tUrls := tUrls
		e.Go(func() error {
			for _, url := range tUrls {
				res, err := d.callTorrentUrlProvider(ctx, url)
				if err != nil {
					d.logger.Warn("[downloader] callTorrentUrlProvider", "err", err)
					continue
				}
				if err := saveTorrent(tPath, res); err != nil {
					d.logger.Warn("[downloader] saveTorrent", "err", err)
					continue
				}
				return nil
			}
			return nil
		})
	}
	if err := e.Wait(); err != nil {
		d.logger.Warn("[downloader] webseed discover", "err", err)
	}
}

func (d *WebSeeds) TorrentUrls() snaptype.TorrentUrls {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.torrentUrls
}

func (d *WebSeeds) ByFileName(name string) (metainfo.UrlList, bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	v, ok := d.byFileName[name]
	return v, ok
}
func (d *WebSeeds) callWebSeedsProvider(ctx context.Context, webSeedProviderUrl *url.URL) (snaptype.WebSeedsFromProvider, error) {
	request, err := http.NewRequest(http.MethodGet, webSeedProviderUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}
	return response, nil
}
func (d *WebSeeds) callTorrentUrlProvider(ctx context.Context, url *url.URL) (*metainfo.MetaInfo, error) {
	request, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response := &metainfo.MetaInfo{}
	if err := toml.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}
	return response, nil
}
func (d *WebSeeds) readWebSeedsFile(webSeedProviderPath string) (snaptype.WebSeedsFromProvider, error) {
	data, err := os.ReadFile(webSeedProviderPath)
	if err != nil {
		return nil, err
	}
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	return response, nil
}
