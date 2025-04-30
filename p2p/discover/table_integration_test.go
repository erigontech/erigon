// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package discover

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"
)

func TestTable_bumpNoDuplicates_quickCheck(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

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
	if testing.Short() {
		t.Skip()
	}

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
	if testing.Short() {
		t.Skip()
	}

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
