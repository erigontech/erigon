package downloader

import (
	"testing"
)

func TestName(t *testing.T) {
	t.Skip("wip")
	tester := newStagedSyncTester(true)
	if err := tester.newPeer("peer", 65, testChainForkLightA); err != nil {
		t.Fatal(err)
	}
	if err := tester.sync("peer", nil); err != nil {
		t.Fatal(err)
	}
}
