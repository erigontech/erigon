package downloader

import (
	"context"
	"fmt"
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

	logger    log.Logger
	verbosity log.Lvl
}

func (d *WebSeeds) Discover(ctx context.Context, s3tokens []string, urls []*url.URL, files []string, rootDir string) {
	d.downloadWebseedTomlFromProviders(ctx, s3tokens, urls, files)
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
	request, err := http.NewRequest(http.MethodGet, webSeedProviderUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("webseed.http: host=%s, url=%s, %w", webSeedProviderUrl.Hostname(), webSeedProviderUrl.EscapedPath(), err)
	}
	defer resp.Body.Close()
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("webseed.http: host=%s, url=%s, %w", webSeedProviderUrl.Hostname(), webSeedProviderUrl.EscapedPath(), err)
	}
	d.logger.Debug("[snapshots.webseed] get from HTTP provider", "urls", len(response), "host", webSeedProviderUrl.Hostname(), "url", webSeedProviderUrl.EscapedPath())
	return response, nil
}
func (d *WebSeeds) callS3Provider(ctx context.Context, token string) (snaptype.WebSeedsFromProvider, error) {
	//v1:bucketName:accID:accessKeyID:accessKeySecret
	l := strings.Split(token, ":")
	if len(l) != 5 {
		return nil, fmt.Errorf("[snapshots] webseed token has invalid format. expeting 5 parts, found %d", len(l))
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
