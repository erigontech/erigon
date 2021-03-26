// Copyright 2015 The go-ethereum Authors
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

package downloader

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

const OverwriteBlockCacheItems = 1024
const OverwriteMaxForkAncestry = 3000

// Reduce some of the parameters to make the tester faster.
func init() {
	blockCacheMaxItems = OverwriteBlockCacheItems
	fsHeaderSafetyNet = 256
	fsHeaderContCheck = 50 * time.Millisecond
	fullMaxForkAncestry = OverwriteMaxForkAncestry
	fsHeaderContCheck = 500 * time.Millisecond
}

func TestRemoteHeaderRequestSpan(t *testing.T) {
	testCases := []struct {
		remoteHeight uint64
		localHeight  uint64
		expected     []int
	}{
		// Remote is way higher. We should ask for the remote head and go backwards
		{1500, 1000,
			[]int{1323, 1339, 1355, 1371, 1387, 1403, 1419, 1435, 1451, 1467, 1483, 1499},
		},
		{15000, 13006,
			[]int{14823, 14839, 14855, 14871, 14887, 14903, 14919, 14935, 14951, 14967, 14983, 14999},
		},
		// Remote is pretty close to us. We don't have to fetch as many
		{1200, 1150,
			[]int{1149, 1154, 1159, 1164, 1169, 1174, 1179, 1184, 1189, 1194, 1199},
		},
		// Remote is equal to us (so on a fork with higher td)
		// We should get the closest couple of ancestors
		{1500, 1500,
			[]int{1497, 1499},
		},
		// We're higher than the remote! Odd
		{1000, 1500,
			[]int{997, 999},
		},
		// Check some weird edgecases that it behaves somewhat rationally
		{0, 1500,
			[]int{0, 2},
		},
		{6000000, 0,
			[]int{5999823, 5999839, 5999855, 5999871, 5999887, 5999903, 5999919, 5999935, 5999951, 5999967, 5999983, 5999999},
		},
		{0, 0,
			[]int{0, 2},
		},
	}
	reqs := func(from, count, span int) []int {
		var r []int
		num := from
		for len(r) < count {
			r = append(r, num)
			num += span + 1
		}
		return r
	}
	for i, tt := range testCases {
		from, count, span, max := calculateRequestSpan(tt.remoteHeight, tt.localHeight)
		data := reqs(int(from), count, span)

		if max != uint64(data[len(data)-1]) {
			t.Errorf("test %d: wrong last value %d != %d", i, data[len(data)-1], max)
		}
		failed := false
		if len(data) != len(tt.expected) {
			failed = true
			t.Errorf("test %d: length wrong, expected %d got %d", i, len(tt.expected), len(data))
		} else {
			for j, n := range data {
				if n != tt.expected[j] {
					failed = true
					break
				}
			}
		}
		if failed {
			res := strings.Replace(fmt.Sprint(data), " ", ",", -1)
			exp := strings.Replace(fmt.Sprint(tt.expected), " ", ",", -1)
			t.Logf("got: %v\n", res)
			t.Logf("exp: %v\n", exp)
			t.Errorf("test %d: wrong values", i)
		}
	}
}

func TestDataRace(t *testing.T) {
	wg := &sync.WaitGroup{}

	const N = 10
	wg.Add(N)
	ln := getTestChainBase().len()
	for i := 0; i < N; i++ {
		go makeFork(wg, ln, i)
	}
	wg.Wait()
}

func makeFork(wg *sync.WaitGroup, ln, i int) {
	getTestChainBase().makeFork(ln, false, uint8(i))
	wg.Done()
}
