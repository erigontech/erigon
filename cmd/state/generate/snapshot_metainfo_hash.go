package generate

import (
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"time"
)

func MetaInfoHash(path string) error {
	t := time.Now()
	mi := metainfo.MetaInfo{}
	info, err := bittorrent.BuildInfoBytesForLMDBSnapshot(path)
	if err != nil {
		return err
	}
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		return err
	}

	fmt.Println(mi.HashInfoBytes())
	fmt.Println("It took", time.Since(t))
	return nil
}
