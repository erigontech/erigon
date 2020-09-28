package torrent

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"testing"
)

var (
	builtinAnnounceList = [][]string{
		{"udp://tracker.openbittorrent.com:80"},
		{"udp://tracker.publicbt.com:80"},
		{"udp://tracker.istole.it:6969"},
	}
)


func TestTorrentAddTorrent(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	path:=os.TempDir()+"/trnt_test3"
	os.RemoveAll(path)

	kv:=ethdb.NewLMDB().Path(path+"/lmdb").MustOpen()
	db:=ethdb.NewObjectDatabase(kv)

	cli:= New(path, SnapshotMode{
		Headers: true,
	}, true)
	err:=cli.Run(db)
	if err!=nil {
		t.Fatal(err)
	}
}



func   TestTorrent(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	path:=os.TempDir()+"/trnt_test"
	kv:=ethdb.NewLMDB().Path(path+"/lmdb").MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	os.RemoveAll(path)
	cli:= New(path, SnapshotMode{
		Headers: true,
	}, true)
	err:=cli.DownloadHeadersSnapshot(db)
	if err!=nil {
		t.Fatal(err)
	}
}

func TestTorrentBodies(t *testing.T) {
	path:=os.TempDir()+"/trnt_test2"
	//os.RemoveAll(path)
	//os.RemoveAll(path+"_pc")
	kv:=ethdb.NewLMDB().Path(path+"/lmdb").MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	cli:= New(path, SnapshotMode{
		Bodies: true,
	}, true)
	err:=cli.DownloadBodiesSnapshot(db)
	if err!=nil {
		t.Fatal(err)
	}
}
