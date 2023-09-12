package snaptype

import "github.com/anacrolix/torrent/metainfo"

// Each provider can provide only 1 WebSeed url per file
// but overall BitTorrent protocol allowing multiple
type WebSeedsFromProvider map[string]string // fileName -> Url, can be Http/Ftp

type WebSeeds map[string]metainfo.UrlList // fileName -> []Url, can be Http/Ftp

func NewWebSeeds(list []WebSeedsFromProvider) WebSeeds {
	merged := WebSeeds{}
	for _, m := range list {
		for name, wUrl := range m {
			merged[name] = append(merged[name], wUrl)
		}
	}
	return merged
}
