// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tests

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate gencodec -type DifficultyTest -field-override difficultyTestMarshaling -out gen_difficultytest.go

type DifficultyTest struct {
	ParentTimestamp    uint64   `json:"parentTimestamp"`
	ParentDifficulty   *big.Int `json:"parentDifficulty"`
	ParentUncles       uint64   `json:"parentUncles"`
	CurrentTimestamp   uint64   `json:"currentTimestamp"`
	CurrentBlockNumber uint64   `json:"currentBlockNumber"`
	CurrentDifficulty  *big.Int `json:"currentDifficulty"`
}

type difficultyTestMarshaling struct {
	ParentTimestamp    math.HexOrDecimal64
	ParentDifficulty   *math.HexOrDecimal256
	CurrentTimestamp   math.HexOrDecimal64
	CurrentDifficulty  *math.HexOrDecimal256
	ParentUncles       uint64
	CurrentBlockNumber math.HexOrDecimal64
}

func (test *DifficultyTest) Run(config *chain.Config) error {
	parentNumber := big.NewInt(int64(test.CurrentBlockNumber - 1))
	parent := &types.Header{
		Difficulty: test.ParentDifficulty,
		Time:       test.ParentTimestamp,
		Number:     parentNumber,
	}

	if test.ParentUncles == 0 {
		parent.UncleHash = types.EmptyUncleHash
	} else {
		parent.UncleHash = libcommon.HexToHash("ab") // some dummy != EmptyUncleHash
	}

	actual := ethash.CalcDifficulty(config, test.CurrentTimestamp, parent.Time, parent.Difficulty, parent.Number.Uint64(), parent.UncleHash)
	exp := test.CurrentDifficulty

	if actual.Cmp(exp) != 0 {
		return fmt.Errorf("parent[time %v diff %v unclehash:%x] child[time %v number %v] diff %v != expected %v",
			test.ParentTimestamp, test.ParentDifficulty, test.ParentUncles,
			test.CurrentTimestamp, test.CurrentBlockNumber, actual, exp)
	}
	return nil

}
