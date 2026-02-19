// Recursive int32-text → int32-SA functions for SAIS.
// Copied from Go stdlib index/suffixarray/sais2.go (Go 1.24),
// keeping only the _32 variants (no _64).
//
// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sais

import (
	"bytes"
	"unsafe"
)

func sais_32(text []int32, textMax int, sa, tmp []int32) {
	if len(sa) != len(text) || len(tmp) < textMax {
		panic("sais: misuse of sais_32")
	}

	if len(text) == 0 {
		return
	}
	if len(text) == 1 {
		sa[0] = 0
		return
	}

	var freq, bucket []int32
	if len(tmp) >= 2*textMax {
		freq, bucket = tmp[:textMax], tmp[textMax:2*textMax]
		freq[0] = -1
	} else {
		freq, bucket = nil, tmp[:textMax]
	}

	numLMS := placeLMS_32(text, sa, freq, bucket)
	if numLMS <= 1 {
		// 0 or 1 items are already sorted.
	} else {
		induceSubL_32(text, sa, freq, bucket)
		induceSubS_32(text, sa, freq, bucket)
		length_32(text, sa, numLMS)
		maxID := assignID_32(text, sa, numLMS)
		if maxID < numLMS {
			map_32(sa, numLMS)
			recurse_32(sa, tmp, numLMS, maxID)
			unmap_32(text, sa, numLMS)
		} else {
			copy(sa, sa[len(sa)-numLMS:])
		}
		expand_32(text, freq, bucket, sa, numLMS)
	}
	induceL_32(text, sa, freq, bucket)
	induceS_32(text, sa, freq, bucket)

	tmp[0] = -1
}

func freq_32(text []int32, freq, bucket []int32) []int32 {
	if freq != nil && freq[0] >= 0 {
		return freq
	}
	if freq == nil {
		freq = bucket
	}
	clear(freq)
	for _, c := range text {
		freq[c]++
	}
	return freq
}

func bucketMin_32(text []int32, freq, bucket []int32) {
	freq = freq_32(text, freq, bucket)
	total := int32(0)
	for i, n := range freq {
		bucket[i] = total
		total += n
	}
}

func bucketMax_32(text []int32, freq, bucket []int32) {
	freq = freq_32(text, freq, bucket)
	total := int32(0)
	for i, n := range freq {
		total += n
		bucket[i] = total
	}
}

func placeLMS_32(text []int32, sa, freq, bucket []int32) int {
	bucketMax_32(text, freq, bucket)

	numLMS := 0
	lastB := int32(-1)

	c0, c1, isTypeS := int32(0), int32(0), false
	for i := len(text) - 1; i >= 0; i-- {
		c0, c1 = text[i], c0
		if c0 < c1 {
			isTypeS = true
		} else if c0 > c1 && isTypeS {
			isTypeS = false

			b := bucket[c1] - 1
			bucket[c1] = b
			sa[b] = int32(i + 1)
			lastB = b
			numLMS++
		}
	}

	if numLMS > 1 {
		sa[lastB] = 0
	}
	return numLMS
}

func induceSubL_32(text []int32, sa, freq, bucket []int32) {
	bucketMin_32(text, freq, bucket)

	k := len(text) - 1
	c0, c1 := text[k-1], text[k]
	if c0 < c1 {
		k = -k
	}

	cB := c1
	b := bucket[cB]
	sa[b] = int32(k)
	b++

	for i := 0; i < len(sa); i++ {
		j := int(sa[i])
		if j == 0 {
			continue
		}
		if j < 0 {
			sa[i] = int32(-j)
			continue
		}
		sa[i] = 0

		k := j - 1
		c0, c1 := text[k-1], text[k]
		if c0 < c1 {
			k = -k
		}

		if cB != c1 {
			bucket[cB] = b
			cB = c1
			b = bucket[cB]
		}
		sa[b] = int32(k)
		b++
	}
}

func induceSubS_32(text []int32, sa, freq, bucket []int32) {
	bucketMax_32(text, freq, bucket)

	cB := int32(0)
	b := bucket[cB]

	top := len(sa)
	for i := len(sa) - 1; i >= 0; i-- {
		j := int(sa[i])
		if j == 0 {
			continue
		}
		sa[i] = 0
		if j < 0 {
			top--
			sa[top] = int32(-j)
			continue
		}

		k := j - 1
		c1 := text[k]
		c0 := text[k-1]
		if c0 > c1 {
			k = -k
		}

		if cB != c1 {
			bucket[cB] = b
			cB = c1
			b = bucket[cB]
		}
		b--
		sa[b] = int32(k)
	}
}

func length_32(text []int32, sa []int32, numLMS int) {
	end := 0

	c0, c1, isTypeS := int32(0), int32(0), false
	for i := len(text) - 1; i >= 0; i-- {
		c0, c1 = text[i], c0
		if c0 < c1 {
			isTypeS = true
		} else if c0 > c1 && isTypeS {
			isTypeS = false

			j := i + 1
			var code int32
			if end == 0 {
				code = 0
			} else {
				code = int32(end - j)
			}
			sa[j>>1] = code
			end = j + 1
		}
	}
}

func assignID_32(text []int32, sa []int32, numLMS int) int {
	id := 0
	lastLen := int32(-1)
	lastPos := int32(0)
	for _, j := range sa[len(sa)-numLMS:] {
		n := sa[j/2]
		if n != lastLen {
			goto New
		}
		if uint32(n) >= uint32(len(text)) {
			goto Same
		}
		{
			n := int(n)
			this := unsafe.Slice((*byte)(unsafe.Pointer(&text[j])), n*4)
			last := unsafe.Slice((*byte)(unsafe.Pointer(&text[lastPos])), n*4)
			if !bytes.Equal(this, last) {
				goto New
			}
			goto Same
		}
	New:
		id++
		lastPos = j
		lastLen = n
	Same:
		sa[j/2] = int32(id)
	}
	return id
}

func unmap_32(text []int32, sa []int32, numLMS int) {
	unmap := sa[len(sa)-numLMS:]
	j := len(unmap)

	c0, c1, isTypeS := int32(0), int32(0), false
	for i := len(text) - 1; i >= 0; i-- {
		c0, c1 = text[i], c0
		if c0 < c1 {
			isTypeS = true
		} else if c0 > c1 && isTypeS {
			isTypeS = false

			j--
			unmap[j] = int32(i + 1)
		}
	}

	sa = sa[:numLMS]
	for i := 0; i < len(sa); i++ {
		sa[i] = unmap[sa[i]]
	}
}

func expand_32(text []int32, freq, bucket, sa []int32, numLMS int) {
	bucketMax_32(text, freq, bucket)

	x := numLMS - 1
	saX := sa[x]
	c := text[saX]
	b := bucket[c] - 1
	bucket[c] = b

	for i := len(sa) - 1; i >= 0; i-- {
		if i != int(b) {
			sa[i] = 0
			continue
		}
		sa[i] = saX

		if x > 0 {
			x--
			saX = sa[x]
			c = text[saX]
			b = bucket[c] - 1
			bucket[c] = b
		}
	}
}

func induceL_32(text []int32, sa, freq, bucket []int32) {
	bucketMin_32(text, freq, bucket)

	k := len(text) - 1
	c0, c1 := text[k-1], text[k]
	if c0 < c1 {
		k = -k
	}

	cB := c1
	b := bucket[cB]
	sa[b] = int32(k)
	b++

	for i := 0; i < len(sa); i++ {
		j := int(sa[i])
		if j <= 0 {
			continue
		}

		k := j - 1
		c1 := text[k]
		if k > 0 {
			if c0 := text[k-1]; c0 < c1 {
				k = -k
			}
		}

		if cB != c1 {
			bucket[cB] = b
			cB = c1
			b = bucket[cB]
		}
		sa[b] = int32(k)
		b++
	}
}

func induceS_32(text []int32, sa, freq, bucket []int32) {
	bucketMax_32(text, freq, bucket)

	cB := int32(0)
	b := bucket[cB]

	for i := len(sa) - 1; i >= 0; i-- {
		j := int(sa[i])
		if j >= 0 {
			continue
		}

		j = -j
		sa[i] = int32(j)

		k := j - 1
		c1 := text[k]
		if k > 0 {
			if c0 := text[k-1]; c0 <= c1 {
				k = -k
			}
		}

		if cB != c1 {
			bucket[cB] = b
			cB = c1
			b = bucket[cB]
		}
		b--
		sa[b] = int32(k)
	}
}
