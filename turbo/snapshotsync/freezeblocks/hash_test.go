package freezeblocks

import (
	"testing"
)

func TestIntegrityCheck(t *testing.T) {
	testFiles := []string{
		// "/Users/steven/erigon-data/snapshots/mainnet/v1-006500-007000-transactions.seg",
		// "/Users/steven/erigon-data/snapshots/mainnet/v1-013000-013500-headers.seg",
		// "/Users/steven/erigon-data/snapshots/mainnet/v1-020410-020420-headers.seg",
	}

	for _, file := range testFiles {
		complete, err := fileIntegrityCheck(file)
		if err != nil {
			t.Fatal(err)
		}
		if !complete {
			t.Errorf("File %s is not complete", file)
		} else {
			t.Logf("File %s is complete", file)
		}
	}
}
