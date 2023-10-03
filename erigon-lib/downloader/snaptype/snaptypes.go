package snaptype

import (
	"net/url"
	"strings"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/log/v3"
)

// Each provider can provide only 1 WebSeed url per file
// but overall BitTorrent protocol allowing multiple
type WebSeedsFromProvider map[string]string // fileName -> Url, can be Http/Ftp

type WebSeedUrls map[string]metainfo.UrlList // fileName -> []Url, can be Http/Ftp
type TorrentUrls map[string][]*url.URL

func NewWebSeedUrls(list []WebSeedsFromProvider) (wsu WebSeedUrls, tu TorrentUrls) {
	for _, urls := range list {
		for name, wUrl := range urls {
			if strings.HasSuffix(name, ".torrent") {
				uri, err := url.ParseRequestURI(wUrl)
				if err != nil {
					log.Debug("[downloader] url is invalid", "url", wUrl, "err", err)
					continue
				}
				tu[name] = append(tu[name], uri)
				continue
			}
			wsu[name] = append(wsu[name], wUrl)
		}
	}
	return wsu, tu
}
