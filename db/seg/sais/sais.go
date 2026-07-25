// Suffix array construction by induced sorting (SAIS).
// Copied from Go stdlib index/suffixarray (Go 1.24), keeping only
// the uint16-text → int32-SA path (_16_32 functions, in sais_16.go).
// The _32 recursive functions live in sais_inner.go.
//
// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sais

import (
	"slices"
)

// copy of stdlib `index/suffixarray` SA-IS implementation
// because Go's stdlib doesn't provide enough low-level api to call necessary funcs
// also for Erigon - it's important to keep control on files reproducibility

// Sais16 computes the suffix array of a uint16 text (alphabet [0,textMax)) into sa.
func Sais16(text []uint16, textMax int, sa []int32, buf *[]int32) error {
	n := len(text)
	if n != len(sa) {
		panic("sais: len(text) != len(sa)")
	}
	if n <= 1 {
		if n == 1 {
			sa[0] = 0
		}
		return nil
	}
	clear(sa)

	needed := max(2*textMax, n/2)
	*buf = slices.Grow((*buf)[:0], needed)[:needed]
	sais_16_32(text, textMax, sa, *buf)
	return nil
}

func map_32(sa []int32, numLMS int) {
	w := len(sa)
	for i := len(sa) / 2; i >= 0; i-- {
		j := sa[i]
		if j > 0 {
			w--
			sa[w] = j - 1
		}
	}
}

func recurse_32(sa, oldTmp []int32, numLMS, maxID int) {
	dst, saTmp, text := sa[:numLMS], sa[numLMS:len(sa)-numLMS], sa[len(sa)-numLMS:]

	tmp := oldTmp
	if len(tmp) < len(saTmp) {
		tmp = saTmp
	}
	if len(tmp) < numLMS {
		tmp = make([]int32, max(maxID, numLMS/2))
	}

	clear(dst)
	sais_32(text, maxID, dst, tmp)
}
