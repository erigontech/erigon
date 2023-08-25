package tests

import (
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
)

func TestExecutionSpec(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)

	dir := filepath.Join(".", "execution-spec-tests")

	// Probably failing due to pre-Byzantium receipts
	bt.skipLoad(`^frontier/`)
	bt.skipLoad(`^homestead/`)

	// TODO(yperbasis): fix me
	bt.skipLoad(`^cancun/`)

	// TODO(yperbasis): re-enable checkStateRoot
	checkStateRoot := false

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, checkStateRoot)); err != nil {
			t.Error(err)
		}
	})
}
