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

import "math/rand"

type Seed int64

func MathRand(seed int64) Seed { return Seed(seed) }

func (s Seed) rand() *rand.Rand { return rand.New(rand.NewSource(int64(s))) }

type Op struct {
	Key                  []byte
	Account              *AccountValue
	Storage              []byte
	Delete               bool
	Read                 bool
	DeleteStorageSubtree bool
}

type Assertions struct {
	TolerateHPHDrift bool
	StateReadEngines []string
	ZeroAccountReads bool
	ZeroStorageReads bool
}

type Case struct {
	ID         string
	Seed       Seed
	Rounds     [][]Op
	Assertions Assertions
}
