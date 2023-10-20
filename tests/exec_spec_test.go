//go:build integration

package tests

import (
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

func TestExecutionSpec(t *testing.T) {
	if ethconfig.EnableHistoryV3InTest {
		t.Skip("fix me in e3 please")
	}

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)

	dir := filepath.Join(".", "execution-spec-tests")

	// TODO(yperbasis): re-enable when execution-spec-tests are updated for the official trusted setup
	// See https://github.com/ethereum/execution-spec-tests/pull/336
	bt.skipLoad(`^cancun/eip4844_blobs/point_evaluation_precompile/`)

	checkStateRoot := true

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, checkStateRoot)); err != nil {
			t.Error(err)
		}
	})
}
