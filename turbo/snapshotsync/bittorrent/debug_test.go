package bittorrent

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"os"
	"testing"
)

func TestTorrentAddTorrent(t *testing.T) {
	t.Skip()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	path := os.TempDir() + "/trnt_test3"
	os.RemoveAll(path)

	kv := ethdb.NewLMDB().Path(path + "/lmdb").MustOpen()
	db := ethdb.NewObjectDatabase(kv)

	cli := New(path, true)
	err := cli.AddTorrent(context.Background(), db, snapshotsync.SnapshotType_headers, params.MainnetChainConfig.ChainID.Uint64())
	if err != nil {
		t.Fatal(err)
	}
}
