package spectest

import (
	"os"
	"testing"

	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/transition"

	"github.com/erigontech/erigon/cl/spectest/consensus_tests"
)

func Test(t *testing.T) {
	spectest.RunCases(t, consensus_tests.TestFormats, transition.ValidatingMachine, os.DirFS("./tests"))
}
