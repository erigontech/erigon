package main

import (
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/consensus_tests"
	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/spectest"
)

func Test(t *testing.T) {
	spectest.RunCases(t, consensus_tests.TestFormats, os.DirFS("./tests"))
}
