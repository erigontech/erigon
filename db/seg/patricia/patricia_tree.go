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

package patricia

import (
	"cmp"
	"fmt"
	"math/bits"
	"slices"
	"strings"
)

type node struct {
	val any
	n0  *node
	n1  *node
	p0  uint32
	p1  uint32
}

func tostr(x uint32) string {
	str := fmt.Sprintf("%b", x)
	for len(str) < 32 {
		str = "0" + str
	}
	return str[:x&0x1f]
}

func (n *node) print(sb *strings.Builder, indent string) {
	sb.WriteString(indent)
	fmt.Fprintf(sb, "%p ", n)
	sb.WriteString(tostr(n.p0))
	sb.WriteString("\n")
	if n.n0 != nil {
		n.n0.print(sb, indent+"    ")
	}
	sb.WriteString(indent)
	fmt.Fprintf(sb, "%p ", n)
	sb.WriteString(tostr(n.p1))
	sb.WriteString("\n")
	if n.n1 != nil {
		n.n1.print(sb, indent+"    ")
	}
	if n.val != nil {
		sb.WriteString(indent)
		sb.WriteString("val:")
		fmt.Fprintf(sb, " %x", n.val.([]byte))
		sb.WriteString("\n")
	}
}

func (n *node) String() string {
	var sb strings.Builder
	n.print(&sb, "")
	return sb.String()
}

// pathWalker represents a position anywhere inside patricia tree.
// position can be identified by combination of node, and the partitioning
// of that node's p0 or p1 into head and tail.
// As with p0 and p1, head and tail are encoded as follows:
// lowest 5 bits encode the length in bits, and the remaining 27 bits
// encode the actual head or tail.
type pathWalker struct {
	n    *node
	head uint32
	tail uint32
}

func (s *pathWalker) String() string {
	return fmt.Sprintf("%p head %s tail %s", s.n, tostr(s.head), tostr(s.tail))
}

func (s *pathWalker) reset(n *node) {
	s.n = n
	s.head = 0
	s.tail = 0
}

func newPathWalker(n *node) *pathWalker {
	return &pathWalker{n: n, head: 0, tail: 0}
}

func (s *pathWalker) transition(b byte, readonly bool) uint32 {
	bitsLeft := 8
	b32 := uint32(b) << 24
	for bitsLeft > 0 {
		if s.head == 0 {
			if b32&0x80000000 == 0 {
				s.tail = s.n.p0
			} else {
				s.tail = s.n.p1
			}
		}
		if s.tail == 0 {
			return b32 | uint32(bitsLeft)
		}
		tailLen := int(s.tail & 0x1f)
		firstDiff := bits.LeadingZeros32(s.tail ^ b32)
		if firstDiff < bitsLeft {
			if firstDiff >= tailLen {
				bitsLeft -= tailLen
				b32 <<= tailLen
				if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
					if s.n.n0 == nil {
						panic("")
					}
					s.n = s.n.n0
				} else {
					if s.n.n1 == nil {
						panic("")
					}
					s.n = s.n.n1
				}
				s.head = 0
				s.tail = 0
			} else {
				bitsLeft -= firstDiff
				b32 <<= firstDiff
				mask := ^(uint32(1)<<(32-firstDiff) - 1)
				s.head |= (s.tail & mask) >> (s.head & 0x1f)
				s.head += uint32(firstDiff)
				s.tail = (s.tail&0xffffffe0)<<firstDiff | (s.tail & 0x1f)
				s.tail -= uint32(firstDiff)
				return b32 | uint32(bitsLeft)
			}
		} else if tailLen < bitsLeft {
			bitsLeft -= tailLen
			b32 <<= tailLen
			if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
				if s.n.n0 == nil {
					if readonly {
						return b32 | uint32(bitsLeft)
					}
					s.n.n0 = &node{}
					if b32&0x80000000 == 0 {
						s.n.n0.p0 = b32 | uint32(bitsLeft)
					} else {
						s.n.n0.p1 = b32 | uint32(bitsLeft)
					}
				}
				s.n = s.n.n0
			} else {
				if s.n.n1 == nil {
					if readonly {
						return b32 | uint32(bitsLeft)
					}
					s.n.n1 = &node{}
					if b32&0x80000000 == 0 {
						s.n.n1.p0 = b32 | uint32(bitsLeft)
					} else {
						s.n.n1.p1 = b32 | uint32(bitsLeft)
					}
				}
				s.n = s.n.n1
			}
			s.head = 0
			s.tail = 0
		} else {
			mask := ^(uint32(1)<<(32-bitsLeft) - 1)
			s.head |= (s.tail & mask) >> (s.head & 0x1f)
			s.head += uint32(bitsLeft)
			s.tail = (s.tail&0xffffffe0)<<bitsLeft | (s.tail & 0x1f)
			s.tail -= uint32(bitsLeft)
			bitsLeft = 0
			if s.tail == 0 {
				if s.head&0x80000000 == 0 {
					if s.n.n0 != nil {
						s.n = s.n.n0
						s.head = 0
					}
				} else {
					if s.n.n1 != nil {
						s.n = s.n.n1
						s.head = 0
					}
				}
			}
		}
	}
	return 0
}

func (s *pathWalker) diverge(divergence uint32) {
	if s.tail == 0 {
		dLen := int(divergence & 0x1f)
		headLen := int(s.head & 0x1f)
		d32 := divergence & 0xffffffe0
		if headLen+dLen > 27 {
			mask := ^(uint32(1)<<(headLen+5) - 1)
			s.head |= (d32 & mask) >> headLen
			s.head += uint32(27 - headLen)
			var dn node
			if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
				s.n.p0 = s.head
				s.n.n0 = &dn
			} else {
				s.n.p1 = s.head
				s.n.n1 = &dn
			}
			s.n = &dn
			s.head = 0
			s.tail = 0
			d32 <<= 27 - headLen
			dLen -= 27 - headLen
			headLen = 0
		}
		mask := ^(uint32(1)<<(32-dLen) - 1)
		s.head |= (d32 & mask) >> headLen
		s.head += uint32(dLen)
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			s.n.p0 = s.head
		} else {
			s.n.p1 = s.head
		}
		return
	}
	var dn node
	if divergence&0x80000000 == 0 {
		dn.p0 = divergence
		dn.p1 = s.tail
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			dn.n1 = s.n.n0
		} else {
			dn.n1 = s.n.n1
		}
	} else {
		dn.p1 = divergence
		dn.p0 = s.tail
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			dn.n0 = s.n.n0
		} else {
			dn.n0 = s.n.n1
		}
	}
	if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
		s.n.n0 = &dn
		s.n.p0 = s.head
	} else {
		s.n.n1 = &dn
		s.n.p1 = s.head
	}
	s.n = &dn
	s.head = divergence
	s.tail = 0
}

func (n *node) insert(key []byte, value any) {
	s := newPathWalker(n)
	for _, b := range key {
		divergence := s.transition(b, false)
		if divergence != 0 {
			s.diverge(divergence)
		}
	}
	s.insert(value)
}

func (s *pathWalker) insert(value any) {
	if s.tail != 0 {
		s.diverge(0)
	}
	if s.head != 0 {
		var dn node
		if s.head&0x80000000 == 0 {
			s.n.n0 = &dn
		} else {
			s.n.n1 = &dn
		}
		s.n = &dn
		s.head = 0
	}
	s.n.val = value
}

func (n *node) get(key []byte) (any, bool) {
	s := newPathWalker(n)
	for _, b := range key {
		divergence := s.transition(b, true)
		if divergence != 0 {
			return nil, false
		}
	}
	if s.tail != 0 {
		return nil, false
	}
	return s.n.val, s.n.val != nil
}

type PatriciaTree struct {
	root node
}

func (pt *PatriciaTree) Insert(key []byte, value any) {
	pt.root.insert(key, value)
}

func (pt *PatriciaTree) Get(key []byte) (any, bool) {
	return pt.root.get(key)
}

type Match struct {
	Val   any
	Start int
	End   int
}

type Matches []Match

func deduplicateMatches(matches Matches) Matches {
	if len(matches) < 2 {
		return matches
	}
	slices.SortFunc(matches, func(i, j Match) int { return cmp.Compare(i.Start, j.Start) })
	lastEnd := matches[0].End
	j := 1
	for _, m := range matches[1:] {
		if m.End > lastEnd {
			matches[j] = m
			lastEnd = m.End
			j++
		}
	}
	return matches[:j]
}
