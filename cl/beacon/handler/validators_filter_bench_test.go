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

package handler

import (
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const benchRegistrySize = 200_000

func benchRegistry() (*solid.ValidatorSet, solid.Uint64ListSSZ) {
	validators := solid.NewValidatorSetWithLength(benchRegistrySize, benchRegistrySize)
	balances := solid.NewUint64ListSSZ(benchRegistrySize)
	for range benchRegistrySize {
		balances.Append(32_000_000_000)
	}
	return validators, balances
}

// benchFilters covers a single id, a handful of ids and a large list of ids
// past the registry, which used to be quadratic.
func benchFilters() map[string][]uint64 {
	many := make([]uint64, 20_000)
	for i := range many {
		many[i] = uint64(benchRegistrySize + i)
	}
	few := make([]uint64, 8)
	for i := range few {
		few[i] = uint64(i * benchRegistrySize / len(few))
	}
	return map[string][]uint64{
		"ids=1":     {benchRegistrySize / 2},
		"ids=8":     few,
		"ids=20000": many,
	}
}

func BenchmarkResponseValidatorsFiltered(b *testing.B) {
	validators, balances := benchRegistry()
	for name, filter := range benchFilters() {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				responseValidators(httptest.NewRecorder(), filter, nil, 0, balances, validators, false, false)
			}
		})
	}
}

func BenchmarkResponseValidatorsBalancesFiltered(b *testing.B) {
	_, balances := benchRegistry()
	for name, filter := range benchFilters() {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				responseValidatorsBalances(filter, balances, false, false)
			}
		})
	}
}
