package torrent

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
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

	cli := New(path, SnapshotMode{
		Headers: true,
	}, true)
	err := cli.Run(db)
	if err != nil {
		t.Fatal(err)
	}
}
