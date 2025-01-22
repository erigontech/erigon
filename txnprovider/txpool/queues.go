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

package txpool

import "github.com/holiman/uint256"

// bestSlice - is similar to best queue, but uses a linear structure with O(n log n) sort complexity and
// it maintains element.bestIndex field
type bestSlice struct {
	ms             []*metaTxn
	pendingBaseFee uint64
}

func (s *bestSlice) Len() int {
	return len(s.ms)
}

func (s *bestSlice) Swap(i, j int) {
	s.ms[i], s.ms[j] = s.ms[j], s.ms[i]
	s.ms[i].bestIndex, s.ms[j].bestIndex = i, j
}

func (s *bestSlice) Less(i, j int) bool {
	return s.ms[i].better(s.ms[j], *uint256.NewInt(s.pendingBaseFee))
}

func (s *bestSlice) UnsafeRemove(i *metaTxn) {
	s.Swap(i.bestIndex, len(s.ms)-1)
	s.ms[len(s.ms)-1].bestIndex = -1
	s.ms[len(s.ms)-1] = nil
	s.ms = s.ms[:len(s.ms)-1]
}

func (s *bestSlice) UnsafeAdd(i *metaTxn) {
	i.bestIndex = len(s.ms)
	s.ms = append(s.ms, i)
}

type BestQueue struct {
	ms             []*metaTxn
	pendingBastFee uint64
}

func (p *BestQueue) Len() int {
	return len(p.ms)
}

func (p *BestQueue) Less(i, j int) bool {
	return p.ms[i].better(p.ms[j], *uint256.NewInt(p.pendingBastFee))
}

func (p *BestQueue) Swap(i, j int) {
	p.ms[i], p.ms[j] = p.ms[j], p.ms[i]
	p.ms[i].bestIndex = i
	p.ms[j].bestIndex = j
}

func (p *BestQueue) Push(x interface{}) {
	n := len(p.ms)
	item := x.(*metaTxn)
	item.bestIndex = n
	p.ms = append(p.ms, item)
}

func (p *BestQueue) Pop() interface{} {
	old := p.ms
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.bestIndex = -1     // for safety
	item.currentSubPool = 0 // for safety
	p.ms = old[0 : n-1]
	return item
}

type WorstQueue struct {
	ms             []*metaTxn
	pendingBaseFee uint64
}

func (p *WorstQueue) Len() int {
	return len(p.ms)
}

func (p *WorstQueue) Less(i, j int) bool {
	return p.ms[i].worse(p.ms[j], *uint256.NewInt(p.pendingBaseFee))
}

func (p *WorstQueue) Swap(i, j int) {
	p.ms[i], p.ms[j] = p.ms[j], p.ms[i]
	p.ms[i].worstIndex = i
	p.ms[j].worstIndex = j
}

func (p *WorstQueue) Push(x interface{}) {
	n := len(p.ms)
	item := x.(*metaTxn)
	item.worstIndex = n
	p.ms = append(p.ms, x.(*metaTxn))
}

func (p *WorstQueue) Pop() interface{} {
	old := p.ms
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.worstIndex = -1    // for safety
	item.currentSubPool = 0 // for safety
	p.ms = old[0 : n-1]
	return item
}
