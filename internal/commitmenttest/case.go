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

func Corpus() []Case {
	cases := make([]Case, 0, 3)
	for _, shape := range []string{"accounts", "storage"} {
		c, err := Generate(MathRand(0), SequenceSpec{Kind: shape, Count: 100000})
		if err != nil {
			panic(err)
		}
		c.ID = "E102/process-100k-" + shape
		c.Assertions.ProcessNoError = true
		cases = append(cases, c)
	}
	c, err := Generate(MathRand(0), SequenceSpec{Kind: "incremental"})
	if err != nil {
		panic(err)
	}
	c.ID = "E103/differential-zero-state-reads"
	c.Assertions = Assertions{ProcessNoError: true, ZeroAccountReads: true, ZeroStorageReads: true, StateReadEngines: []string{"v3"}}
	cases = append(cases, c)
	return cases
}
