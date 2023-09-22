//go:build integration

package tests

import (
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

func TestExecutionSpec(t *testing.T) {
	if ethconfig.EnableHistoryV3InTest {
		t.Skip("fix me in e3 please")
	}

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)

	dir := filepath.Join(".", "execution-spec-tests")

	// TODO(yperbasis): fix me
	bt.skipLoad(`^cancun/eip4844_blobs/excess_blob_gas/`)

	checkStateRoot := true

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, checkStateRoot)); err != nil {
			t.Error(err)
		}
	})
}
