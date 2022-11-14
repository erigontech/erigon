//go:build integration

package discover

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"
)

func TestTable_bumpNoDuplicates_quickCheck(t *testing.T) {
	t.Parallel()

	config := quick.Config{
		MaxCount: 200,
		Rand:     rand.New(rand.NewSource(time.Now().Unix())),
	}

	test := func(bucketCountGen byte, bumpCountGen byte) bool {
		return testTableBumpNoDuplicatesRun(t, bucketCountGen, bumpCountGen, config.Rand)
	}

	if err := quick.Check(test, &config); err != nil {
		t.Error(err)
	}
}

func TestTable_findNodeByID_quickCheck(t *testing.T) {
	t.Parallel()

	config := quick.Config{
		MaxCount: 1000,
		Rand:     rand.New(rand.NewSource(time.Now().Unix())),
	}

	test := func(nodesCount uint16, resultsCount byte) bool {
		return testTableFindNodeByIDRun(t, nodesCount, resultsCount, config.Rand)
	}

	if err := quick.Check(test, &config); err != nil {
		t.Error(err)
	}
}

func TestTable_ReadRandomNodesGetAll_quickCheck(t *testing.T) {
	t.Parallel()

	config := quick.Config{
		MaxCount: 200,
		Rand:     rand.New(rand.NewSource(time.Now().Unix())),
	}

	test := func(nodesCount uint16) bool {
		return testTableReadRandomNodesGetAllRun(t, nodesCount, config.Rand)
	}

	if err := quick.Check(test, &config); err != nil {
		t.Error(err)
	}
}
