//go:build spectest

// once all tests are implemented, we can allow this test in the ci build path

package spectest

import (
	"github.com/ledgerwatch/erigon/cl/spectest/consensus_tests"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/spectest"
)

func Test(t *testing.T) {
	spectest.RunCases(t, consensus_tests.TestFormats, os.DirFS("./tests"))
}
