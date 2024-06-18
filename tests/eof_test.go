package tests

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon-lib/log/v3"
)

func TestEOFv1(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	et := new(testMatcher)

	dir := filepath.Join(".", "eof_tests/prague/eip6206_jumpf")

	et.walk(t, dir, func(t *testing.T, name string, test *EOFTest) {
		// import pre accounts & construct test genesis block & state root
		if err := et.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
		fmt.Println("---------------------------------")
	})
}
