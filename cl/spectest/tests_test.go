package spectest

import (
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/transition"

	"github.com/ledgerwatch/erigon/cl/spectest/consensus_tests"

	"github.com/ledgerwatch/erigon/spectest"
)

func Test(t *testing.T) {
	spectest.RunCases(t, consensus_tests.TestFormats, transition.ValidatingMachine, os.DirFS("./tests"))
}
