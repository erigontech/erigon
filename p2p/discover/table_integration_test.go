//go:build integration
// +build integration

package discover

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"
)

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
