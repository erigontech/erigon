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

package commitment

import (
	"bytes"
	"cmp"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

const presortBuckets = 256

const presortBucketKeep = 8192

const presortParallelMin = 4096

type presortEntry struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
	seq       uint32
}

type presorter struct {
	buckets [presortBuckets][]presortEntry
	count   int
	seq     uint32
}

func presortBucketOf(hashedKey []byte) int {
	switch len(hashedKey) {
	case 0:
		return 0
	case 1:
		return int(hashedKey[0]&0x0f) << 4
	default:
		return int(hashedKey[0]&0x0f)<<4 | int(hashedKey[1]&0x0f)
	}
}

func (p *presorter) collect(hashedKey, plainKey []byte, update *Update) {
	i := presortBucketOf(hashedKey)
	p.buckets[i] = append(p.buckets[i], presortEntry{
		hashedKey: hashedKey,
		plainKey:  plainKey,
		update:    update,
		seq:       p.seq,
	})
	p.seq++
	p.count++
}

func presortLess(a, b presortEntry) int {
	if c := bytes.Compare(a.hashedKey, b.hashedKey); c != 0 {
		return c
	}
	return cmp.Compare(a.seq, b.seq)
}

func (p *presorter) sortBuckets() {
	nw := min(runtime.GOMAXPROCS(0), presortBuckets, 1+p.count/presortParallelMin)
	if nw <= 1 || p.count < presortParallelMin {
		for i := range p.buckets {
			if len(p.buckets[i]) > 1 {
				slices.SortFunc(p.buckets[i], presortLess)
			}
		}
		return
	}
	var next atomic.Int32
	var wg sync.WaitGroup
	wg.Add(nw)
	for range nw {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= presortBuckets {
					return
				}
				if len(p.buckets[i]) > 1 {
					slices.SortFunc(p.buckets[i], presortLess)
				}
			}
		}()
	}
	wg.Wait()
}

func (p *presorter) releaseBucket(i int) {
	if cap(p.buckets[i]) > presortBucketKeep {
		p.buckets[i] = nil
		return
	}
	clear(p.buckets[i])
	p.buckets[i] = p.buckets[i][:0]
}

func (p *presorter) reset() {
	for i := range p.buckets {
		p.releaseBucket(i)
	}
	p.count, p.seq = 0, 0
}
