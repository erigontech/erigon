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

package heimdall

type ClosedRange struct {
	Start uint64
	End   uint64
}

func (r ClosedRange) Len() uint64 {
	return r.End + 1 - r.Start
}

func ClosedRangeMap[TResult any](r ClosedRange, projection func(i uint64) (TResult, error)) ([]TResult, error) {
	results := make([]TResult, 0, r.Len())

	for i := r.Start; i <= r.End; i++ {
		entity, err := projection(i)
		if err != nil {
			return nil, err
		}

		results = append(results, entity)
	}

	return results, nil
}

func (r ClosedRange) Map(projection func(i uint64) (any, error)) ([]any, error) {
	return ClosedRangeMap(r, projection)
}
