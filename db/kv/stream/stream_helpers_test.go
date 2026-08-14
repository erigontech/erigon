// Copyright 2021 The Erigon Authors
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

package stream

import (
	"fmt"
)

// PairsWithErrorIter - return N, keys and then error
type PairsWithErrorIter struct {
	errorAt, i int
}

func PairsWithError(errorAt int) *PairsWithErrorIter {
	return &PairsWithErrorIter{errorAt: errorAt}
}
func (m *PairsWithErrorIter) Close()        {}
func (m *PairsWithErrorIter) HasNext() bool { return true }
func (m *PairsWithErrorIter) Next() ([]byte, []byte, error) {
	if m.i >= m.errorAt {
		return nil, nil, fmt.Errorf("expected error at iteration: %d", m.errorAt)
	}
	m.i++
	return fmt.Appendf(nil, "%x", m.i), fmt.Appendf(nil, "%x", m.i), nil
}
