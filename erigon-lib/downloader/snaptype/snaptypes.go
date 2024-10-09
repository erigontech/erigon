package snaptype

import (
	"net/url"

	"github.com/anacrolix/torrent/metainfo"
)

// Each provider can provide only 1 WebSeed url per file
// but overall BitTorrent protocol allowing multiple
type WebSeedsFromProvider map[string]string // fileName -> Url, can be Http/Ftp

type WebSeedUrls map[string]metainfo.UrlList // fileName -> []Url, can be Http/Ftp
type TorrentUrls map[string][]*url.URL
