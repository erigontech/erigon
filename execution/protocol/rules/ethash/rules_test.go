// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package ethash

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
)

type diffTest struct {
	ParentTimestamp    uint64
	ParentDifficulty   uint256.Int
	CurrentTimestamp   uint64
	CurrentBlocknumber uint256.Int
	CurrentDifficulty  uint256.Int
}

func (d *diffTest) UnmarshalJSON(b []byte) (err error) {
	var ext struct {
		ParentTimestamp    string
		ParentDifficulty   string
		CurrentTimestamp   string
		CurrentBlocknumber string
		CurrentDifficulty  string
	}
	if err := json.Unmarshal(b, &ext); err != nil {
		return err
	}

	d.ParentTimestamp = math.MustParseUint64(ext.ParentTimestamp)
	d.ParentDifficulty = *uint256.MustFromHex(ext.ParentDifficulty)
	d.CurrentTimestamp = math.MustParseUint64(ext.CurrentTimestamp)
	d.CurrentBlocknumber = *uint256.MustFromHex(ext.CurrentBlocknumber)
	d.CurrentDifficulty = *uint256.MustFromHex(ext.CurrentDifficulty)

	return nil
}

func TestCalcDifficulty(t *testing.T) {
	file, err := os.Open(filepath.Join("..", "..", "tests", "testdata", "BasicTests", "difficulty.json"))
	if err != nil {
		t.Skip(err)
	}
	defer file.Close()

	tests := make(map[string]diffTest)
	err = json.NewDecoder(file).Decode(&tests)
	if err != nil {
		t.Fatal(err)
	}

	config := &chain.Config{HomesteadBlock: big.NewInt(1150000)}

	for name, test := range tests {
		number := test.CurrentBlocknumber.Uint64() - 1
		diff := CalcDifficulty(config, test.CurrentTimestamp,
			test.ParentTimestamp,
			test.ParentDifficulty,
			number,
			empty.UncleHash,
		)
		if diff.Cmp(&test.CurrentDifficulty) != 0 {
			t.Error(name, "failed. Expected", &test.CurrentDifficulty, "and calculated", &diff)
		}
	}
}
