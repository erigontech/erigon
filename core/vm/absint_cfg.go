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

package vm

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/holiman/uint256"
)

////////////////////////

const (
	BotValue AbsValueKind = iota
	TopValue
	InvalidValue
	ConcreteValue
)

func (d AbsValueKind) String() string {
	return [...]string{"⊥", "⊤", "x", "AbsValue"}[d]
}

func (d AbsValueKind) hash() uint64 {
	if d == BotValue {
		return 0
	} else if d == TopValue {
		return 1
	} else if d == InvalidValue {
		return 2
	} else if d == ConcreteValue {
		return 3
	} else {
		panic("no hash found")
	}
}

//////////////////////////////////////////////////

type AbsValue struct {
	kind  AbsValueKind
	value *uint256.Int //only when kind=ConcreteValue
	pc    int          //only when kind=TopValue
}

func (c0 AbsValue) String(abbrev bool) string {
	if c0.kind == InvalidValue {
		return c0.kind.String()
	} else if c0.kind == BotValue {
		return c0.kind.String()
	} else if c0.kind == TopValue {
		if !abbrev {
			return fmt.Sprintf("%v%v", c0.kind.String(), c0.pc)
		}
		return c0.kind.String()
	} else if c0.value.IsUint64() {
		return strconv.FormatUint(c0.value.Uint64(), 10)
	}
	return "256bit"
}

func AbsValueTop(pc int) AbsValue {
	return AbsValue{kind: TopValue, pc: pc}
}

func AbsValueInvalid() AbsValue {
	return AbsValue{kind: InvalidValue}
}

func AbsValueConcrete(value uint256.Int) AbsValue {
	return AbsValue{kind: ConcreteValue, value: &value}
}

func (c0 AbsValue) Eq(c1 AbsValue) bool {
	if c0.kind != c1.kind {
		return false
	}

	if c0.kind == ConcreteValue {
		if !c0.value.Eq(c1.value) {
			return false
		}
	}

	return true
}

func (c0 AbsValue) hash() uint64 {
	hash := 47 * c0.kind.hash()
	if c0.kind == ConcreteValue {
		hash += 57 * uint256Hash(c0.value)
	}
	return hash
}

func (c0 AbsValue) Stringify() string {
	if c0.kind == InvalidValue || c0.kind == TopValue {
		return c0.kind.String()
	} else if c0.kind == ConcreteValue {
		b, err := c0.value.MarshalText()
		if err != nil {
			log.Fatal("Can't unmarshall")
		}
		return string(b)
	}

	log.Fatal("Invalid abs value kind")
	return ""
}

func AbsValueDestringify(s string) AbsValue {
	if s == "⊤" {
		return AbsValueTop(-1)
	} else if s == "x" {
		return AbsValueInvalid()
	} else if strings.HasPrefix(s, "0x") {
		var i uint256.Int
		err := i.UnmarshalText([]byte(s))
		if err != nil {
			log.Fatal("Can't unmarshall")
		}
		return AbsValueConcrete(i)
	}

	log.Fatal("Invalid abs value kind")
	return AbsValue{}
}

// ////////////////////////////////////////////////
type astack struct {
	values []AbsValue
	hash   uint64
}

func newStack() *astack {
	st := &astack{}
	st.updateHash()
	return st
}

func (s *astack) Copy() *astack {
	newStack := &astack{}
	newStack.values = append(newStack.values, s.values...)
	newStack.hash = s.hash
	return newStack
}

func uint256Hash(e *uint256.Int) uint64 {
	return 19*e[0] + 23*e[1] + 29*e[2]*37*e[3]
}

func (s *astack) updateHash() {
	s.hash = 0
	for k, e := range s.values {
		s.hash += uint64(k) * e.hash()
	}
}

func (s *astack) Push(value AbsValue) {
	rest := s.values
	s.values = nil
	s.values = append(s.values, value)
	s.values = append(s.values, rest...)
	s.updateHash()
}

func (s *astack) Pop(pc int) AbsValue {
	res := s.values[0]
	s.values = s.values[1:len(s.values)]
	//s.values = append(s.values, AbsValueTop(pc, true))
	s.updateHash()
	return res
}

func (s *astack) String(abbrev bool) string {
	strs := make([]string, 0)
	for _, c := range s.values {
		strs = append(strs, c.String(abbrev))
	}
	return strings.Join(strs, " ")
}

func (s *astack) Eq(s1 *astack) bool {
	if s.hash != s1.hash {
		return false
	}

	if len(s.values) != len(s1.values) {
		return false
	}

	for i := 0; i < len(s.values); i++ {
		if !s.values[i].Eq(s1.values[i]) {
			return false
		}
	}
	return true
}

func (s *astack) hasIndices(i ...int) bool {
	for _, i := range i {
		if !(i < len(s.values)) {
			return false
		}
	}
	return true
}

//////////////////////////////////////////////////

type astate struct {
	stackset    []*astack
	anlyCounter int
	worklistLen int
}

func emptyState() *astate {
	return &astate{stackset: nil, anlyCounter: -1, worklistLen: -1}
}

func (state *astate) Copy() *astate {
	newState := emptyState()
	for _, stack := range state.stackset {
		newState.stackset = append(newState.stackset, stack.Copy())
	}
	return newState
}

func botState() *astate {
	st := emptyState()

	botStack := newStack()
	st.stackset = append(st.stackset, botStack)

	return st
}

func ExistsIn(values []AbsValue, value AbsValue) bool {
	for _, v := range values {
		if value.Eq(v) {
			return true
		}
	}
	return false
}

func (state *astate) String(abbrev bool) string {
	maxStackLen := 0
	for _, stack := range state.stackset {
		if maxStackLen < len(stack.values) {
			maxStackLen = len(stack.values)
		}
	}

	var elms []string
	for i := 0; i < maxStackLen; i++ {
		var elm []string
		var values []AbsValue
		for _, stack := range state.stackset {
			if stack.hasIndices(i) {
				value := stack.values[i]
				if !ExistsIn(values, value) {
					elm = append(elm, value.String(abbrev))
					values = append(values, value)
				}
			}
		}

		var e string
		if len(values) > 1 {
			e = fmt.Sprintf("{%v}", strings.Join(elm, ","))
		} else {
			e = strings.Join(elm, ",")
		}
		elms = append(elms, e)
	}

	elms = append(elms, fmt.Sprintf("%v%v%v", "|", len(state.stackset), "|"))
	return strings.Join(elms, " ")
}

func (state *astate) Add(stack *astack) {
	for _, existing := range state.stackset {
		if existing.Eq(stack) {
			return
		}
	}
	state.stackset = append(state.stackset, stack)
}

//////////////////////////////////////////////////

// -1 block id is invalid jump
type CfgProofState struct {
	Pc     int
	Stacks [][]string
}

type CfgProofBlock struct {
	Entry *CfgProofState
	Exit  *CfgProofState
	Preds []int
	Succs []int
}

type CfgProof struct {
	Blocks []*CfgProofBlock
}

func DeserializeCfgProof(proofBytes []byte) *CfgProof {
	proof := CfgProof{}
	err := json.Unmarshal(DecompressProof(proofBytes), &proof)
	if err != nil {
		log.Fatal("Cannot deserialize proof")
	}
	return &proof
}

func (proof *CfgProof) Serialize() []byte {
	res, err := json.MarshalIndent(*proof, "", " ")
	if err != nil {
		log.Fatal("Cannot serialize proof")
	}
	return CompressProof(res)
}

func CompressProof(in []byte) []byte {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(in)
	if err != nil {
		log.Fatal("cannot write proof")
	}
	err = w.Close()
	if err != nil {
		log.Fatal("cannot close file")
	}
	return b.Bytes()
}

func DecompressProof(in []byte) []byte {
	reader := bytes.NewReader(in)
	breader, err := zlib.NewReader(reader)
	if err != nil {
		log.Fatal("cannot read")
	}

	res, err := io.ReadAll(breader)
	if err != nil {
		log.Fatal("cannot read")
	}
	return res
}

func (proof *CfgProof) ToString() string {
	return string(proof.Serialize())
}

// block.{Entry|Exit}.Pc in code, block.{Succs|Preds} in some block.{Entry}.Pc
// Entry <= Exit
// No overlap of blocks
// Must have block starting at 0 with a empty state
// Succs,Preds consistency
// No duplicate succs
// No duplicate preds
// succs are sorted
// preds are sorted
func (proof *CfgProof) isValid() bool {
	return true
}

func StringifyAState(st *astate) [][]string {
	stacks := make([][]string, 0)

	for _, astack := range st.stackset {
		var stack []string
		for _, v := range astack.values {
			stack = append(stack, v.Stringify())
		}
		stacks = append(stacks, stack)
	}

	return stacks
}

func intoAState(ststr [][]string) *astate {
	st := astate{}

	for _, stack := range ststr {
		s := astack{}
		for _, vstr := range stack {
			s.values = append(s.values, AbsValueDestringify(vstr))
		}
		s.updateHash()
		st.Add(&s)
	}

	return &st
}

func Leq(st0 *astate, st1 *astate) bool {
	for _, stack0 := range st0.stackset {
		var found bool
		for _, stack1 := range st1.stackset {
			if stack0.Eq(stack1) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func Eq(st0 *astate, st1 *astate) bool {
	return Leq(st0, st1) && Leq(st1, st0)
}

func Lub(st0 *astate, st1 *astate) *astate {
	newState := emptyState()
	for _, stack := range st0.stackset {
		newState.Add(stack)
	}
	for _, stack := range st1.stackset {
		newState.Add(stack)
	}
	return newState
}
