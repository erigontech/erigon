package beacontest_test

import (
	"testing"

	_ "embed"

	"github.com/ledgerwatch/erigon/cl/beacon/beacontest"
)

//go:embed harness_test_data.yml
var testData []byte

func TestSimpleHarness(t *testing.T) {
	beacontest.Execute(
		beacontest.WithTesting(t),
		beacontest.WithTestFromBytes("test", testData),
	)
}
