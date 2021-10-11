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
	"testing"
)

func TestInserts1(t *testing.T) {
	n := &node{}
	s := makestate(n)
	d := s.transition(0x34)
	fmt.Printf("1 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("2 tree:\n%sstate: %s\n\n", n, s)
	d = s.transition(0x56)
	fmt.Printf("3 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("4 tree:\n%sstate: %s\n\n", n, s)
	d = s.transition(0xff)
	fmt.Printf("5 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("6 tree:\n%sstate: %s\n\n", n, s)
	d = s.transition(0xcc)
	fmt.Printf("7 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("8 tree:\n%sstate: %s\n\n", n, s)
	s.insert(nil)
	s = makestate(n)
	d = s.transition(0x34)
	fmt.Printf("9 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	d = s.transition(0x66)
	fmt.Printf("10 tree:\n%sstate: %s\ndivergence %s\n\n", n, s, tostr(d))
	s.diverge(d)
	fmt.Printf("11 tree:\n%sstate: %s\n\n", n, s)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0xff}, []byte{0x01})
	fmt.Printf("12 tree:\n%s\n", n)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0x0f}, []byte{0x02})
	fmt.Printf("13 tree:\n%s\n", n)

	n.insert([]byte{0xff, 0xff, 0xff, 0xff, 0xff}, []byte{0x03})
	fmt.Printf("14 tree:\n%s\n", n)

	vs, ok := n.get([]byte{0xff, 0xff, 0xff, 0xff, 0x0f})
	fmt.Printf("15 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	fmt.Printf("16 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x56})
	fmt.Printf("17 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0x34, 0x56, 0xff, 0xcc})
	fmt.Printf("18 vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{})
	fmt.Printf("19 vs = %v, ok = %t\n", vs, ok)
}

func TestInserts2(t *testing.T) {
	var n node
	n.insert([]byte{0xff}, []byte{0x03, 0x03, 0x03, 0x1a, 0xed, 0xed})
	n.insert([]byte{0xed}, []byte{})
	fmt.Printf("tree:\n%s", &n)

	vs, ok := n.get([]byte{0xff})
	fmt.Printf("vs = %v, ok = %t\n", vs, ok)

	vs, ok = n.get([]byte{0xed})
	fmt.Printf("vs = %v, ok = %t\n", vs, ok)
}

func TestFindMatches1(t *testing.T) {
	var pt PatriciaTree
	pt.Insert([]byte("wolf"), []byte{1})
	pt.Insert([]byte("winter"), []byte{2})
	pt.Insert([]byte("wolfs"), []byte{3})
	fmt.Printf("n\n%s", &pt.root)
	var mf MatchFinder
	matches := mf.FindLongestMatches(&pt, []byte("Who lives here in winter, wolfs?"))
	for _, m := range matches {
		fmt.Printf("%+v\n", m)
	}
}
