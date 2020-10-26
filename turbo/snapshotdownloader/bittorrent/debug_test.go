package bittorrent

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotdownloader"
	"os"
	"testing"
)

func TestTorrentAddTorrent(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	path := os.TempDir() + "/trnt_test3"
	os.RemoveAll(path)

	kv := ethdb.NewLMDB().Path(path + "/lmdb").MustOpen()
	db := ethdb.NewObjectDatabase(kv)

	cli := New(path, true)
	err := cli.AddTorrent(context.Background(), db, snapshotdownloader.HeadersSnapshotName, HeadersSnapshotHash)
	if err != nil {
		t.Fatal(err)
	}
}
