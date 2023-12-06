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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"golang.org/x/sync/errgroup"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/common/dir"
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

	logger    log.Logger
	verbosity log.Lvl
}

func (d *WebSeeds) Discover(ctx context.Context, s3tokens []string, urls []*url.URL, files []string, rootDir string) {
	d.downloadWebseedTomlFromProviders(ctx, s3tokens, urls, files)
	d.downloadTorrentFilesFromProviders(ctx, rootDir)
}

func (d *WebSeeds) downloadWebseedTomlFromProviders(ctx context.Context, s3Providers []string, httpProviders []*url.URL, diskProviders []string) {
	log.Debug("[snapshots] webseed providers", "http", len(httpProviders), "s3", len(s3Providers), "disk", len(diskProviders))
	list := make([]snaptype.WebSeedsFromProvider, 0, len(httpProviders)+len(diskProviders))
	for _, webSeedProviderURL := range httpProviders {
		select {
		case <-ctx.Done():
			break
		default:
		}
		response, err := d.callHttpProvider(ctx, webSeedProviderURL)
		if err != nil { // don't fail on error
			d.logger.Debug("[snapshots.webseed] get from HTTP provider", "err", err, "url", webSeedProviderURL.EscapedPath())
			continue
		}
		list = append(list, response)
	}

	for _, webSeedProviderURL := range s3Providers {
		select {
		case <-ctx.Done():
			break
		default:
		}
		response, err := d.callS3Provider(ctx, webSeedProviderURL)
		if err != nil { // don't fail on error
			d.logger.Debug("[snapshots.webseed] get from S3 provider", "err", err)
			continue
		}
		list = append(list, response)
	}
	// add to list files from disk
	for _, webSeedFile := range diskProviders {
		response, err := d.readWebSeedsFile(webSeedFile)
		if err != nil { // don't fail on error
			d.logger.Debug("[snapshots.webseed] get from File provider", "err", err)
			continue
		}
		list = append(list, response)
	}

	webSeedUrls, torrentUrls := snaptype.WebSeedUrls{}, snaptype.TorrentUrls{}
	for _, urls := range list {
		for name, wUrl := range urls {
			if !strings.HasSuffix(name, ".torrent") {
				webSeedUrls[name] = append(webSeedUrls[name], wUrl)
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
		}
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.byFileName = webSeedUrls
	d.torrentUrls = torrentUrls
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
func (d *WebSeeds) callHttpProvider(ctx context.Context, webSeedProviderUrl *url.URL) (snaptype.WebSeedsFromProvider, error) {
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
		return nil, fmt.Errorf("webseed.http: %w, host=%s, url=%s", err, webSeedProviderUrl.Hostname(), webSeedProviderUrl.EscapedPath())
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("webseed.http: %w, host=%s, url=%s, ", err, webSeedProviderUrl.Hostname(), webSeedProviderUrl.EscapedPath())
	}
	response := snaptype.WebSeedsFromProvider{}
	fileNames := strings.Split(string(b), "\n")
	for _, f := range fileNames {
		response[f], err = url.JoinPath(baseUrl, f)
		if err != nil {
			return nil, err
		}
	}
	d.logger.Debug("[snapshots.webseed] get from HTTP provider", "urls", len(response), "host", webSeedProviderUrl.Hostname(), "url", webSeedProviderUrl.EscapedPath())
	return response, nil
}
func (d *WebSeeds) callS3Provider(ctx context.Context, token string) (snaptype.WebSeedsFromProvider, error) {
	//v1:bucketName:accID:accessKeyID:accessKeySecret
	l := strings.Split(token, ":")
	if len(l) != 5 {
		return nil, fmt.Errorf("token has invalid format, exepcing 'v1:tokenInBase64'")
	}
	version, bucketName, accountId, accessKeyId, accessKeySecret := strings.TrimSpace(l[0]), strings.TrimSpace(l[1]), strings.TrimSpace(l[2]), strings.TrimSpace(l[3]), strings.TrimSpace(l[4])
	if version != "v1" {
		return nil, fmt.Errorf("not supported version: %s", version)
	}
	var fileName = "webseeds.toml"

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId),
		}, nil
	})
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, "")),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	//  {
	//  	"ChecksumAlgorithm": null,
	//  	"ETag": "\"eb2b891dc67b81755d2b726d9110af16\"",
	//  	"Key": "ferriswasm.png",
	//  	"LastModified": "2022-05-18T17:20:21.67Z",
	//  	"Owner": null,
	//  	"Size": 87671,
	//  	"StorageClass": "STANDARD"
	//  }
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucketName, Key: &fileName})
	if err != nil {
		return nil, fmt.Errorf("webseed.s3: bucket=%s, %w", bucketName, err)
	}
	defer resp.Body.Close()
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("webseed.s3: bucket=%s, %w", bucketName, err)
	}
	d.logger.Debug("[snapshots.webseed] get from S3 provider", "urls", len(response), "bucket", bucketName)
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
		if !strings.HasSuffix(name, ".seg.torrent") {
			_, fName := filepath.Split(name)
			d.logger.Log(d.verbosity, "[snapshots] webseed has .torrent, but we skip it because this file-type not supported yet", "name", fName)
			continue
		}
		name := name
		tUrls := tUrls
		e.Go(func() error {
			for _, url := range tUrls {
				res, err := d.callTorrentHttpProvider(ctx, url, name)
				if err != nil {
					d.logger.Log(d.verbosity, "[snapshots] got from webseed", "name", name, "err", err)
					continue
				}
				d.logger.Log(d.verbosity, "[snapshots] got from webseed", "name", name)
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
	fileName = strings.TrimSuffix(fileName, ".torrent")
	for i := 0; i < len(whitelist); i++ {
		if whitelist[i].Name == fileName {
			return true
		}
	}
	return false
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
