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

package jsonrpc

func newCallFromToBlockProvider(isBackwards bool, callFromProvider, callToProvider BlockProvider) BlockProvider {
	var nextFrom, nextTo uint64
	var hasMoreFrom, hasMoreTo bool
	initialized := false

	return func() (uint64, bool, error) {
		if !initialized {
			initialized = true

			var err error
			if nextFrom, hasMoreFrom, err = callFromProvider(); err != nil {
				return 0, false, err
			}
			hasMoreFrom = hasMoreFrom || nextFrom != 0

			if nextTo, hasMoreTo, err = callToProvider(); err != nil {
				return 0, false, err
			}
			hasMoreTo = hasMoreTo || nextTo != 0
		}

		if !hasMoreFrom && !hasMoreTo {
			return 0, false, nil
		}

		var blockNum uint64
		if !hasMoreFrom {
			blockNum = nextTo
		} else if !hasMoreTo {
			blockNum = nextFrom
		} else {
			blockNum = nextFrom
			if isBackwards {
				if nextTo < nextFrom {
					blockNum = nextTo
				}
			} else {
				if nextTo > nextFrom {
					blockNum = nextTo
				}
			}
		}

		// Pull next; it may be that from AND to contains the same blockNum
		if hasMoreFrom && blockNum == nextFrom {
			var err error
			if nextFrom, hasMoreFrom, err = callFromProvider(); err != nil {
				return 0, false, err
			}
			hasMoreFrom = hasMoreFrom || nextFrom != 0
		}
		if hasMoreTo && blockNum == nextTo {
			var err error
			if nextTo, hasMoreTo, err = callToProvider(); err != nil {
				return 0, false, err
			}
			hasMoreTo = hasMoreTo || nextTo != 0
		}
		return blockNum, hasMoreFrom || hasMoreTo, nil
	}
}
