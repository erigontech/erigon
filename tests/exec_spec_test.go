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

	// Failing because the fixture was filled by geth w/o EIP-3860
	bt.skipLoad(`^withdrawals/withdrawals/withdrawals_newly_created_contract.json`)

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, false)); err != nil {
			t.Error(err)
		}
	})
}
