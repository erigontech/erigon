package downloader

import (
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"testing"
)

func TestName(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	tester := newStagedSyncTester(true)
	if err := tester.newPeer("peer", 65, testChainForkLightA); err != nil {
		t.Fatal(err)
	}
	if err := tester.sync("peer", nil); err != nil {
		t.Fatal(err)
	}
}
