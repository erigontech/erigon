package downloader

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"testing"
)

func TestName(t *testing.T) {
	core.UsePlainStateExecution = true
	tester := newStagedSyncTester(true)
	if err := tester.newPeer("peer", 65, testChainForkLightA); err != nil {
		t.Fatal(err)
	}
	if err := tester.sync("peer", nil); err != nil {
		t.Fatal(err)
	}
}
