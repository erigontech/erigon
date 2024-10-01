//go:build integration

package tests

import (
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/log/v3"
)

func TestExecutionSpec(t *testing.T) {
	if config3.EnableHistoryV3InTest {
		t.Skip("fix me in e3 please")
	}

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)

	dir := filepath.Join(".", "execution-spec-tests")

	// TODO(yperbasis) make it work
	bt.skipLoad(`^prague/eip2935_historical_block_hashes_from_state/block_hashes/block_hashes_history.json`)
	bt.skipLoad(`^prague/eip7685_general_purpose_el_requests/`)

	checkStateRoot := true

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, checkStateRoot)); err != nil {
			t.Error(err)
		}
	})
}
