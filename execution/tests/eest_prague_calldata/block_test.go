package eest_prague_calldata_test

import (
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

var eestDir = filepath.Join("..", "..", "..", "test-fixtures-cache", "eest_stable", "fixtures")

func TestExecutionSpecBlockchainPragueCalldata(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testutil.TestMatcher)
	dir := filepath.Join(eestDir, "blockchain_tests")
	bt.Whitelist(`^prague/eip7623_increase_calldata_cost/test_transaction_validity_type_1_type_2\.json`)
	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
