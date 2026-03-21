package eest_osaka_clz_test

import (
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

func TestExecutionSpecBlockchainOsakaCLZ(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testutil.TestMatcher)
	dir := filepath.Join("..", "execution-spec-tests", "blockchain_tests")
	bt.Whitelist(`^osaka/eip7939_count_leading_zeros/test_clz_opcode_scenarios\.json`)
	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
