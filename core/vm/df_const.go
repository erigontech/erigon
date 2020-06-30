package vm

import (
	"fmt"
	"strconv"
)

//////////////////////////////////////////////////

func SimpleConstPropHarness(contract *Contract) {
	/*c := AbsConst{Top,0}
	fmt.Printf("%+v\n", c)
	s := AbsState{}
	s.stack = append(s.stack, c)
	fmt.Printf("%+v\n", s)
	//if true { return &DataFlow{},nil }
	*/
	jt := newIstanbulInstructionSet()

	cfg0, err := ToCfg0(contract, &jt)
	if err != nil {
		panic("Cannot create cfg0")
	}

	spec := UnbSpec{}

	df := NewDataFlow(cfg0, spec)

	err = df.Run()
	if err != nil {
		panic("Dataflow failed")
	}

	df.Print()
}


//////////////////////////////////////////////////
type AbsConstValueType int

const (
	Bot AbsConstValueType = iota
	Top
	Value
)

func (d AbsConstValueType) String() string {
	return [...]string{"Bot", "Top", "Value"}[d]
}

type AbsConst struct {
	kind AbsConstValueType
	value uint64
}

func (c0 AbsConst) String() string {
	if c0.kind == Bot || c0.kind == Top {
		return c0.kind.String()
	} else {
		return strconv.FormatUint(c0.value, 10)
	}
}

func (c0 *AbsConst) Lub(c1 AbsConst) AbsConst {
	if c0.kind == Value && c1.kind == Value {
		if c0.value != c1.value {
			return AbsConst{Top, 0}
		} else {
			return AbsConst{Value,  c0.value}
		}
	} else if c0.kind == Top || c1.kind == Top {
		return AbsConst{Top, 0}
	} else if c0.kind == Bot {
		return AbsConst{Bot, c1.value}
	} else if c1.kind == Bot {
		return AbsConst{Bot, c0.value}
	} else {
		panic("Missing condition")
	}
}


///////////////
//Unbounded stack

type UnbState struct {
	stack []AbsConst
}

func (state0 *UnbState) String() string {
	var stackStr []string
	for _, c := range state0.stack {
		stackStr = append(stackStr, c.String())
	}
	return fmt.Sprintf("%v", stackStr)
}

func (state0 *UnbState) Copy() *UnbState {
	state1 := &UnbState{}
	state1.stack = append([]AbsConst(nil), state0.stack...)
	return state1
}

func (state0 *UnbState) Push(value AbsConst) {
	state0.stack = append([]AbsConst{value}, state0.stack...)
}

func (state0 *UnbState) Pop() {
	state0.stack = state0.stack[1:]
}

func (state0 *UnbState) Canonicalize() {
	st := state0.stack
	for len(st) > 0 && st[len(st)-1].kind == Bot {
		st = st[:len(st)-1]
	}
	state0.stack = st
}

////////////////////////////////////////////////////
type UnbSpec struct {
}

func (spec UnbSpec) Bot() AbsState {
	return &UnbState{}
}


func (spec UnbSpec) Lub (astate0 AbsState, astate1 AbsState) AbsState {
	res := &UnbState{}
	state0 := astate0.(UnbState)
	state1 := astate1.(UnbState)

	if len(state0.stack) > len(state1.stack) {
		state0prev := state0
		state0 = state1
		state1 = state0prev
	}

	for i := 0; i < len(state0.stack); i++ {
		lub := state0.stack[i].Lub(state1.stack[i])
		res.stack = append(res.stack, lub)
	}

	for i := len(state0.stack); i < len(state1.stack); i++ {
		res.stack = append(res.stack, state1.stack[i])
	}

	res.Canonicalize()

	return res
}

func (spec UnbSpec) Eq(astate0 AbsState, astate1 AbsState) bool {
	state0 := astate0.(UnbState)
	state1 := astate1.(UnbState)

	if len(state0.stack) != len(state1.stack) {
		return false
	}

	for i := 0; i < len(state0.stack); i++ {
		if state0.stack[i] != state1.stack[i] {
			return false
		}
	}

	return true
}

func (spec UnbSpec) Transfer(astate0 AbsState, instrNode *Node) AbsState {
	state0 := astate0.(UnbState)
	state1 := state0.Copy()

	if instrNode.opCode.IsPush() {
		state1.Push(*instrNode.opValue)
	} else {
		switch instrNode.opCode {
		case JUMP:
		case JUMPI:
			state1.Pop()
		}
	}

	return state1
}

func (spec UnbSpec) String(astate0 AbsState) string {
	state0 := astate0.(UnbState)
	return state0.String()
}