package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"
)

// WebSeeds - allow use HTTP-based infrastrucutre to support Bittorrent network
// it allows download .torrent files and data files from trusted url's (for example: S3 signed url)
type WebSeeds struct {
	lock sync.Mutex

	byFileName          snaptype.WebSeedUrls // HTTP urls of data files
	torrentUrls         snaptype.TorrentUrls // HTTP urls of .torrent files
	downloadTorrentFile bool

	logger    log.Logger
	verbosity log.Lvl
}

func (d *WebSeeds) Discover(ctx context.Context, urls []*url.URL, files []string, rootDir string) {
	d.downloadWebseedTomlFromProviders(ctx, urls, files)
	d.downloadTorrentFilesFromProviders(ctx, rootDir)
}

func (d *WebSeeds) downloadWebseedTomlFromProviders(ctx context.Context, providers []*url.URL, diskProviders []string) {
	list := make([]snaptype.WebSeedsFromProvider, 0, len(providers)+len(diskProviders))
	for _, webSeedProviderURL := range providers {
		select {
		case <-ctx.Done():
			break
		default:
		}
		response, err := d.callWebSeedsProvider(ctx, webSeedProviderURL)
		if err != nil { // don't fail on error
			d.logger.Debug("[snapshots] downloadWebseedTomlFromProviders", "err", err, "url", webSeedProviderURL.EscapedPath())
			continue
		}
		list = append(list, response)
	}
	// add to list files from disk
	for _, webSeedFile := range diskProviders {
		response, err := d.readWebSeedsFile(webSeedFile)
		if err != nil { // don't fail on error
			_, fileName := filepath.Split(webSeedFile)
			d.logger.Debug("[snapshots] downloadWebseedTomlFromProviders", "err", err, "file", fileName)
			continue
		}
		if len(diskProviders) > 0 {
			d.logger.Log(d.verbosity, "[snapshots] see webseed.toml file", "files", webSeedFile)
		}
		list = append(list, response)
	}

	webSeedUrls, torrentUrls := snaptype.WebSeedUrls{}, snaptype.TorrentUrls{}
	for _, urls := range list {
		for name, wUrl := range urls {
			if strings.HasSuffix(name, ".torrent") {
				uri, err := url.ParseRequestURI(wUrl)
				if err != nil {
					d.logger.Debug("[snapshots] url is invalid", "url", wUrl, "err", err)
					continue
				}
				torrentUrls[name] = append(torrentUrls[name], uri)
				continue
			}
			webSeedUrls[name] = append(webSeedUrls[name], wUrl)
		}
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.byFileName = webSeedUrls
	d.torrentUrls = torrentUrls
}

// downloadTorrentFilesFromProviders - if they are not exist on file-system
func (d *WebSeeds) downloadTorrentFilesFromProviders(ctx context.Context, rootDir string) {
	// TODO: need more tests, need handle more forward-compatibility and backward-compatibility case
	//  - now, if add new type of .torrent files to S3 bucket - existing nodes will start downloading it. maybe need whitelist of file types
	//  - maybe need download new files if --snap.stop=true
	if !d.downloadTorrentFile {
		return
	}
	if len(d.TorrentUrls()) == 0 {
		return
	}
	var addedNew int
	e, ctx := errgroup.WithContext(ctx)
	urlsByName := d.TorrentUrls()
	//TODO:
	// - what to do if node already synced?
	for name, tUrls := range urlsByName {
		tPath := filepath.Join(rootDir, name)
		if dir.FileExist(tPath) {
			continue
		}
		addedNew++
		if strings.HasSuffix(name, ".v.torrent") || strings.HasSuffix(name, ".ef.torrent") {
			_, fName := filepath.Split(name)
			if strings.HasPrefix(fName, "commitment") {
				d.logger.Log(d.verbosity, "[snapshots] webseed has .torrent, but we skip it because we don't support it yet", "name", name)
				continue
			}
		}
		name := name
		tUrls := tUrls
		e.Go(func() error {
			for _, url := range tUrls {
				res, err := d.callTorrentUrlProvider(ctx, url)
				if err != nil {
					d.logger.Debug("[snapshots] callTorrentUrlProvider", "err", err)
					continue
				}
				d.logger.Log(d.verbosity, "[snapshots] downloaded .torrent file from webseed", "name", name)
				if err := saveTorrent(tPath, res); err != nil {
					d.logger.Debug("[snapshots] saveTorrent", "err", err)
					continue
				}
				return nil
			}
			return nil
		})
	}
	if err := e.Wait(); err != nil {
		d.logger.Debug("[snapshots] webseed discover", "err", err)
	}
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
func (d *WebSeeds) callTorrentUrlProvider(ctx context.Context, url *url.URL) ([]byte, error) {
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
	//protect against too small and too big data
	if resp.ContentLength == 0 || resp.ContentLength > int64(128*datasize.MB) {
		return nil, nil
	}
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if err = validateTorrentBytes(res, url.Path); err != nil {
		return nil, err
	}
	return res, nil
}
func validateTorrentBytes(b []byte, url string) error {
	var mi metainfo.MetaInfo
	if err := bencode.NewDecoder(bytes.NewBuffer(b)).Decode(&mi); err != nil {
		return fmt.Errorf("invalid bytes received from url %s, err=%w", url, err)
	}
	return nil
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
