// Copyright 2026 The Erigon Authors
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

package commitmenttest

import (
	"fmt"
	"math/rand"
)

type Seed struct {
	Algorithm string
	Version   uint32
	Words     []uint64
}

func MathRand(seed int64) Seed {
	return Seed{Algorithm: "math/rand", Version: 1, Words: []uint64{uint64(seed)}}
}

func (s Seed) rand() (*rand.Rand, error) {
	if s.Algorithm != "math/rand" || s.Version != 1 || len(s.Words) != 1 {
		return nil, fmt.Errorf("unsupported seed: %+v", s)
	}
	return rand.New(rand.NewSource(int64(s.Words[0]))), nil
}

type Op struct {
	Key     []byte
	Account *AccountValue
	Storage []byte
	Delete  bool
	Read    bool
}

type Assertions struct {
	TolerateHPHDrift bool
	StateReadEngines []string
	ProcessNoError   bool
	ZeroAccountReads bool
	ZeroStorageReads bool
}

type Case struct {
	ID         string
	Seed       Seed
	Rounds     [][]Op
	Shape      Shape
	Records    []RecordSpec
	Assertions Assertions
}
