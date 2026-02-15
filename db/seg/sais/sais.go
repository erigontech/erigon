// Pure Go SA-IS (Suffix Array by Induced Sorting) implementation.
// Adapted from Go standard library index/suffixarray (BSD-3-clause).
// Original: Copyright 2019 The Go Authors. All rights reserved.
// See Ge Nong, Sen Zhang, and Wai Hong Chen,
// "Two Efficient Algorithms for Linear Time Suffix Array Construction".
package sais

import "fmt"

// Sais computes the suffix array of data and stores it in sa.
// len(sa) must equal len(data).
func Sais(data []byte, sa []int32) error {
	if len(data) != len(sa) {
		return fmt.Errorf("sais: len(data)=%d != len(sa)=%d", len(data), len(sa))
	}
	if len(data) == 0 {
		return nil
	}
	if len(data) == 1 {
		sa[0] = 0
		return nil
	}
	clear(sa)
	sais_8_32(data, 256, sa, make([]int32, 2*256))
	return nil
}

func sais_8_32(text []byte, textMax int, sa, tmp []int32) {
	var freq, bucket []int32
	if len(tmp) >= 2*textMax {
		freq, bucket = tmp[:textMax], tmp[textMax:2*textMax]
		freq[0] = -1
	} else {
		freq, bucket = nil, tmp[:textMax]
	}

	numLMS := placeLMS_8_32(text, sa, freq, bucket)
	if numLMS <= 1 {
		// 0 or 1 items are already sorted.
	} else {
		induceSubL_8_32(text, sa, freq, bucket)
		induceSubS_8_32(text, sa, freq, bucket)
		length_8_32(text, sa, numLMS)
		maxID := assignID_8_32(text, sa, numLMS)
		if maxID < numLMS {
			map_32(sa, numLMS)
			recurse_32(sa, tmp, numLMS, maxID)
			unmap_8_32(text, sa, numLMS)
		} else {
			copy(sa, sa[len(sa)-numLMS:])
		}
		expand_8_32(text, freq, bucket, sa, numLMS)
	}
	induceL_8_32(text, sa, freq, bucket)
	induceS_8_32(text, sa, freq, bucket)
	tmp[0] = -1
}

func freq_8_32(text []byte, freq, bucket []int32) []int32 {
	if freq != nil && freq[0] >= 0 {
		return freq
	}
	if freq == nil {
		freq = bucket
	}
	freq = freq[:256]
	clear(freq)
	for _, c := range text {
		freq[c]++
	}
	return freq
}

func bucketMin_8_32(text []byte, freq, bucket []int32) {
	freq = freq_8_32(text, freq, bucket)
	freq = freq[:256]
	bucket = bucket[:256]
	total := int32(0)
	for i, n := range freq {
		bucket[i] = total
		total += n
	}
}

func bucketMax_8_32(text []byte, freq, bucket []int32) {
	freq = freq_8_32(text, freq, bucket)
	freq = freq[:256]
	bucket = bucket[:256]
	total := int32(0)
	for i, n := range freq {
		total += n
		bucket[i] = total
	}
}

func placeLMS_8_32(text []byte, sa, freq, bucket []int32) int {
	bucketMax_8_32(text, freq, bucket)

	numLMS := 0
	lastB := int32(-1)
	bucket = bucket[:256]

	c0, c1, isTypeS := byte(0), byte(0), false
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

func induceSubL_8_32(text []byte, sa, freq, bucket []int32) {
	bucketMin_8_32(text, freq, bucket)
	bucket = bucket[:256]

	k := len(text) - 1
	c0, c1 := text[k-1], text[k]
	if c0 < c1 {
		k = -k
	}

	lastC := c1
	b := bucket[lastC]
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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		sa[b] = int32(k)
		b++
	}
}

func induceSubS_8_32(text []byte, sa, freq, bucket []int32) {
	bucketMax_8_32(text, freq, bucket)
	bucket = bucket[:256]

	lastC := byte(0)
	b := bucket[lastC]

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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		b--
		sa[b] = int32(k)
	}
}

func length_8_32(text []byte, sa []int32, numLMS int) {
	end := 0
	cx := uint32(0)

	c0, c1, isTypeS := byte(0), byte(0), false
	for i := len(text) - 1; i >= 0; i-- {
		c0, c1 = text[i], c0
		cx = cx<<8 | uint32(c1+1)
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
				if code <= 32/8 && ^cx >= uint32(len(text)) {
					code = int32(^cx)
				}
			}
			sa[j>>1] = code
			end = j + 1
			cx = uint32(c1 + 1)
		}
	}
}

func assignID_8_32(text []byte, sa []int32, numLMS int) int {
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
			this := text[j:][:n]
			last := text[lastPos:][:n]
			for i := 0; i < n; i++ {
				if this[i] != last[i] {
					goto New
				}
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

func map_32(sa []int32, numLMS int) {
	dst := len(sa)
	for i := len(sa) / 2; i >= 0; i-- {
		j := sa[i]
		if j > 0 {
			dst--
			sa[dst] = j - 1
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
		n := maxID
		if n < numLMS/2 {
			n = numLMS / 2
		}
		tmp = make([]int32, n)
	}

	clear(dst)
	sais_32(text, maxID, dst, tmp)
}

func unmap_8_32(text []byte, sa []int32, numLMS int) {
	unmap := sa[len(sa)-numLMS:]
	j := len(unmap)

	c0, c1, isTypeS := byte(0), byte(0), false
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

func expand_8_32(text []byte, freq, bucket, sa []int32, numLMS int) {
	bucketMax_8_32(text, freq, bucket)
	bucket = bucket[:256]

	src := numLMS - 1
	val := sa[src]
	c := text[val]
	b := bucket[c] - 1
	bucket[c] = b

	for i := len(sa) - 1; i >= 0; i-- {
		if i != int(b) {
			sa[i] = 0
			continue
		}
		sa[i] = val
		if src > 0 {
			src--
			val = sa[src]
			c = text[val]
			b = bucket[c] - 1
			bucket[c] = b
		}
	}
}

func induceL_8_32(text []byte, sa, freq, bucket []int32) {
	bucketMin_8_32(text, freq, bucket)
	bucket = bucket[:256]

	k := len(text) - 1
	c0, c1 := text[k-1], text[k]
	if c0 < c1 {
		k = -k
	}

	lastC := c1
	b := bucket[lastC]
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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		sa[b] = int32(k)
		b++
	}
}

func induceS_8_32(text []byte, sa, freq, bucket []int32) {
	bucketMax_8_32(text, freq, bucket)
	bucket = bucket[:256]

	lastC := byte(0)
	b := bucket[lastC]

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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		b--
		sa[b] = int32(k)
	}
}

// --- int32 → int32 functions for recursive subproblem ---

func sais_32(text []int32, textMax int, sa, tmp []int32) {
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

	lastC := c1
	b := bucket[lastC]
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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		sa[b] = int32(k)
		b++
	}
}

func induceSubS_32(text []int32, sa, freq, bucket []int32) {
	bucketMax_32(text, freq, bucket)

	lastC := int32(0)
	b := bucket[lastC]

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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
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
			this := text[j:][:n]
			last := text[lastPos:][:n]
			for i := 0; i < n; i++ {
				if this[i] != last[i] {
					goto New
				}
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

	src := numLMS - 1
	val := sa[src]
	c := text[val]
	b := bucket[c] - 1
	bucket[c] = b

	for i := len(sa) - 1; i >= 0; i-- {
		if i != int(b) {
			sa[i] = 0
			continue
		}
		sa[i] = val
		if src > 0 {
			src--
			val = sa[src]
			c = text[val]
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

	lastC := c1
	b := bucket[lastC]
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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		sa[b] = int32(k)
		b++
	}
}

func induceS_32(text []int32, sa, freq, bucket []int32) {
	bucketMax_32(text, freq, bucket)

	lastC := int32(0)
	b := bucket[lastC]

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

		if lastC != c1 {
			bucket[lastC] = b
			lastC = c1
			b = bucket[lastC]
		}
		b--
		sa[b] = int32(k)
	}
}
