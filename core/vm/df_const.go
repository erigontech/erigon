package vm

import (
	"fmt"
	"github.com/holiman/uint256"
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

	spec := BndSpec{k:10}

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
	ConstValueKind
)

func (d AbsConstValueType) String() string {
	return [...]string{"⊥", "⊤", "Value"}[d]
}

type AbsConst struct {
	kind AbsConstValueType
	value uint256.Int
}

func (c0 AbsConst) String() string {
	if c0.kind == Bot || c0.kind == Top {
		return c0.kind.String()
	} else {
		if c0.value.IsUint64() {
			return strconv.FormatUint(c0.value.Uint64(), 10)
		} else {
			return "256bit"
		}
	}
}

func ConstLub(c0 AbsConst, c1 AbsConst) AbsConst {
	if c0.kind == ConstValueKind && c1.kind == ConstValueKind {
		if c0.value != c1.value {
			return ConstTop()
		} else {
			return ConstValue(c0.value)
		}
	} else if c0.kind == Bot && c1.kind == Bot {
		return ConstBot()
	} else if c0.kind == ConstValueKind && c1.kind == Bot {
		return ConstValue(c0.value)
	} else if c0.kind == Bot && c1.kind == ConstValueKind {
		return ConstValue(c1.value)
	} else if c0.kind == Top || c1.kind == Top {
		return ConstTop()
	} else {
		panic("Missing condition")
	}
}

func ConstLeq(c0 AbsConst, c1 AbsConst) bool {
	if c0.kind == Bot || c1.kind == Top {
		return true
	} else if c0.kind == ConstValueKind && c1.kind == ConstValueKind && c0.value.Eq(&c1.value) {
		return true
	} else {
		return false
	}
}

func zero() uint256.Int {
	return *uint256.NewInt().SetUint64(0)
}

func ConstTop() AbsConst {
	return AbsConst{Top, zero()}
}

func ConstBot() AbsConst {
	return AbsConst{Bot, zero()}
}

func ConstValue(value uint256.Int) AbsConst {
	return AbsConst{ConstValueKind, value}
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
		lub := ConstLub(state0.stack[i], state1.stack[i])
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
		case JUMP, JUMPI:
			state1.Pop()
		}
	}

	return state1
}

func (spec UnbSpec) String(astate0 AbsState) string {
	state0 := astate0.(UnbState)
	return state0.String()
}

///////////////
//Bounded stack

type BndState struct {
	stack []AbsConst
}

func (state0 *BndState) String() string {
	var stackStr []string
	for _, c := range state0.stack {
		stackStr = append(stackStr, c.String())
	}
	return fmt.Sprintf("%v", stackStr)
}

func (state0 *BndState) Copy() BndState {
	state1 := BndState{}
	state1.stack = append([]AbsConst(nil), state0.stack...)
	return state1
}

func (state0 *BndState) Push(value AbsConst) {
	state0.stack = append([]AbsConst{value}, state0.stack[0:len(state0.stack)-1]...)
}

func (state0 *BndState) Pop() {
	state0.stack = append(state0.stack[1:], ConstTop())
}

func (state0 *BndState) Peek() AbsConst {
	return state0.stack[0]
}

////////////////////////////////////////////////////
type BndSpec struct {
	k int
}

func (spec BndSpec) Bot() AbsState {
	if spec.k <= 0 {
		panic("k must be positive")
	}

	st := BndState{}
	for i := 0; i < spec.k; i++ {
		st.stack = append(st.stack, ConstBot())
	}
	return st
}


func (spec BndSpec) Lub (astate0 AbsState, astate1 AbsState) AbsState {
	res := BndState{}
	state0 := astate0.(BndState)
	state1 := astate1.(BndState)

	for i := 0; i < spec.k; i++ {
		lub := ConstLub(state0.stack[i], state1.stack[i])
		res.stack = append(res.stack, lub)
	}

	return res
}

func (spec BndSpec) Eq(astate0 AbsState, astate1 AbsState) bool {
	state0 := astate0.(BndState)
	state1 := astate1.(BndState)

	for i := 0; i < spec.k; i++ {
		if state0.stack[i] != state1.stack[i] {
			return false
		}
	}

	return true
}

func (spec BndSpec) Transfer(astate0 AbsState, instrNode *Node) AbsState {
	state0 := astate0.(BndState)
	state1 := state0.Copy()

	if instrNode.opCode.IsPush() {
		state1.Push(*instrNode.opValue)
	} else {
		switch instrNode.opCode {
		case JUMP, JUMPI:
			state1.Pop()
		}
	}

	return state1
}

func (spec BndSpec) String(astate0 AbsState) string {
	state0 := astate0.(BndState)
	return state0.String()
}