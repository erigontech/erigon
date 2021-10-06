/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package patricia

import (
	"fmt"
	"math/bits"
	"strings"
)

// Implementation of paticia tree for efficient search of substrings from a dictionary in a given string

type node struct {
	p0 uint32 // Number of bits in the left prefix is encoded into the lower 5 bits, the remaining 27 bits are left prefix
	p1 uint32 // Number of bits in the right prefix is encoding into the lower 5 bits, the remaining 27 bits are right prefix
	//size   uint64
	n0     *node
	n1     *node
	values [][]byte
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
	if len(n.values) > 0 {
		sb.WriteString(indent)
		sb.WriteString("vals:")
		for _, v := range n.values {
			fmt.Fprintf(sb, " %x", v)
		}
		sb.WriteString("\n")
	}
}

func (n *node) String() string {
	var sb strings.Builder
	n.print(&sb, "")
	return sb.String()
}

type state struct {
	n    *node
	head uint32
	tail uint32
}

func (s *state) String() string {
	return fmt.Sprintf("%p head %s tail %s", s.n, tostr(s.head), tostr(s.tail))
}

func (s *state) reset(n *node) {
	s.n = n
	s.head = 0
	s.tail = 0
}

func makestate(n *node) *state {
	return &state{n: n, head: 0, tail: 0}
}

// transition consumes next byte of the key, moves the state to corresponding
// node of the patricia tree and returns divergence prefix (0 if there is no divergence)
func (s *state) transition(b byte) uint32 {
	bitsLeft := 8 // Bits in b to process
	b32 := uint32(b) << 24
	for bitsLeft > 0 {
		//fmt.Printf("bitsLeft = %d, b32 = %x, s.head = %s, s.tail = %s\n", bitsLeft, b32, tostr(s.head), tostr(s.tail))
		if s.head == 0 {
			// tail has not been determined yet, do it now
			if b32&0x80000000 == 0 {
				s.tail = s.n.p0
			} else {
				s.tail = s.n.p1
			}
			//fmt.Printf("s.tail <- %s\n", tostr(s.tail))
		}
		if s.tail == 0 {
			return b32 | uint32(bitsLeft)
		}
		tailLen := int(s.tail & 0x1f)
		firstDiff := bits.LeadingZeros32(s.tail ^ b32) // First bit where b32 and tail are different
		//fmt.Printf("firstDiff = %d, bitsLeft = %d\n", firstDiff, bitsLeft)
		if firstDiff < bitsLeft {
			//fmt.Printf("firstDiff < bitsLeft\n")
			if firstDiff >= tailLen {
				//fmt.Printf("firstDiff >= tailLen\n")
				bitsLeft -= tailLen
				b32 <<= tailLen
				// Need to switch to the next node
				if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
					if s.n.n0 == nil {
						//fmt.Printf("new node n0\n")
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
						//fmt.Printf("new node n1\n")
						s.n.n1 = &node{}
						if b32&0x80000000 == 0 {
							s.n.n1.p0 = b32 | uint32(bitsLeft)
						} else {
							s.n.n1.p1 = b32 | uint32(bitsLeft)
						}
					}
					s.n = s.n.n1
				}
				//fmt.Printf("s.n = %p\n", s.n)
				s.head = 0
				s.tail = 0
			} else {
				bitsLeft -= firstDiff
				b32 <<= firstDiff
				// there is divergence, move head and tail
				mask := ^(uint32(1)<<(32-firstDiff) - 1)
				s.head |= (s.tail & mask) >> (s.head & 0x1f)
				s.head += uint32(firstDiff)
				s.tail = (s.tail&0xffffffe0)<<firstDiff | (s.tail & 0x1f)
				s.tail -= uint32(firstDiff)
				return b32 | uint32(bitsLeft)
			}
		} else if tailLen < bitsLeft {
			//fmt.Printf("tailLen < bitsLeft\n")
			bitsLeft -= tailLen
			b32 <<= tailLen
			// Switch to the next node
			if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
				if s.n.n0 == nil {
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
			// key byte is consumed
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

func (s *state) diverge(divergence uint32) {
	if s.tail == 0 {
		// try to add to the existing head
		//fmt.Printf("adding divergence to existing head\n")
		dLen := int(divergence & 0x1f)
		headLen := int(s.head & 0x1f)
		d32 := divergence & 0xffffffe0
		//fmt.Printf("headLen %d + dLen %d = %d\n", headLen, dLen, headLen+dLen)
		if headLen+dLen > 27 {
			mask := ^(uint32(1)<<(headLen+5) - 1)
			//fmt.Printf("mask = %b\n", mask)
			s.head |= (d32 & mask) >> headLen
			s.head += uint32(27 - headLen)
			//fmt.Printf("s.head %s\n", tostr(s.head))
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
			dLen -= (27 - headLen)
			headLen = 0
		}
		//fmt.Printf("headLen %d + dLen %d = %d\n", headLen, dLen, headLen+dLen)
		mask := ^(uint32(1)<<(32-dLen) - 1)
		//fmt.Printf("mask = %b\n", mask)
		s.head |= (d32 & mask) >> headLen
		s.head += uint32(dLen)
		//fmt.Printf("s.head %s\n", tostr(s.head))
		if (s.head == 0 && s.tail&0x80000000 == 0) || (s.head != 0 && s.head&0x80000000 == 0) {
			s.n.p0 = s.head
		} else {
			s.n.p1 = s.head
		}
		return
	}
	// create a new node
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

func (n *node) insert(key []byte, value []byte) {
	s := makestate(n)
	for _, b := range key {
		divergence := s.transition(b)
		if divergence != 0 {
			s.diverge(divergence)
		}
	}
	s.insert(value)
}

func (s *state) insert(value []byte) {
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
	s.n.values = append(s.n.values, value)
}

func (n *node) get(key []byte) ([][]byte, bool) {
	s := makestate(n)
	for _, b := range key {
		divergence := s.transition(b)
		//fmt.Printf("get %x, b = %x, divergence = %s\nstate=%s\n", key, b, tostr(divergence), s)
		if divergence != 0 {
			return nil, false
		}
	}
	if s.tail != 0 {
		return nil, false
	}
	return s.n.values, len(s.n.values) > 0
}

type PatriciaTree struct {
	root node
}

func (pt *PatriciaTree) Insert(key []byte, value []byte) {
	pt.root.insert(key, value)
}

func (pt PatriciaTree) Get(key []byte) ([][]byte, bool) {
	return pt.root.get(key)
}

type Match struct {
	Start int
	End   int
	Vals  [][]byte
}

type MatchFinder struct {
	s       state
	matches []Match
}

func (mf *MatchFinder) FindLongestMatches(pt *PatriciaTree, data []byte) []Match {
	matchCount := 0
	s := &mf.s
	lastEnd := 0
	for start := 0; start < len(data)-1; start++ {
		s.reset(&pt.root)
		emitted := false
		for end := start + 1; end < len(data); end++ {
			if d := s.transition(data[end-1]); d == 0 {
				if s.tail == 0 && len(s.n.values) > 0 && end > lastEnd {
					var m *Match
					if !emitted {
						if matchCount == len(mf.matches) {
							mf.matches = append(mf.matches, Match{})
							m = &mf.matches[len(mf.matches)-1]
						} else {
							m = &mf.matches[matchCount]
						}
						matchCount++
						emitted = true
					} else {
						m = &mf.matches[matchCount-1]
					}
					// This possible overwrites previous match for the same start position
					m.Start = start
					m.End = end
					m.Vals = s.n.values
					lastEnd = end
				}
			} else {
				break
			}
		}
	}
	return mf.matches[:matchCount]
}
