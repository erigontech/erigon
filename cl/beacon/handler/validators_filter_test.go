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

func BenchmarkResponseValidatorsFiltered(b *testing.B) {
	const registry = 200_000
	validators := solid.NewValidatorSetWithLength(registry, registry)
	balances := solid.NewUint64ListSSZ(registry)
	for range registry {
		balances.Append(32_000_000_000)
	}
	filter := make([]uint64, 20_000)
	for i := range filter {
		filter[i] = uint64(registry + i)
	}
	b.ResetTimer()
	for range b.N {
		responseValidators(httptest.NewRecorder(), filter, nil, 0, balances, validators, false, false)
	}
}
