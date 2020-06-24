package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"sort"
	"strconv"
)

type Cfg struct {
	pcList []uint64
	nodes map[uint64]*Node
}

func (cfg *Cfg) Print() {
	keys := make([]uint64, 0)
	for k, _ := range cfg.nodes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _,k := range keys {
		n := cfg.nodes[k]
		pcsuccs := make([]uint64, 0)
		for _, s := range n.succs {
			pcsuccs = append(pcsuccs, s.pc)
		}
		fmt.Printf("%-6d %-6v\n", k, pcsuccs)
	}
}

type NodeSet map[uint64]*Node

type Node struct {
	pc uint64
	opCode OpCode
	opValue *AbsConst
	succs NodeSet
	preds NodeSet
}

func NewNode(pc uint64, opCode OpCode) *Node {
	return &Node {pc, opCode, nil, make(NodeSet), make(NodeSet)}
}

func NewCfg() *Cfg {
	cfg := &Cfg{}
	cfg.nodes = make(map[uint64]*Node)
	return cfg
}

func Cfg0Harness(contract *Contract) {
	jt := newIstanbulInstructionSet()
	cfg, err := ToCfg0(contract, &jt)
	if err != nil {
		panic(err)
	}

	cfg.Print()
}

func ToCfg0(contract *Contract, jt *JumpTable) (cfg *Cfg, err error) {
	cfg = NewCfg()

	pc := uint64(0)
	jumps := make([]*Node, 0)
	var lastNode *Node = nil
	codeLen := len(contract.Code)
	for int(pc) < codeLen {
		op := contract.GetOp(pc)
		node := NewNode(pc, op)

		cfg.nodes[pc] = node
		cfg.pcList = append(cfg.pcList, pc)

		opLength := 1
		operation := &jt[op]
		if !operation.valid {
			return nil, &ErrInvalidOpCode{opcode: op}
		}

		if op.IsPush() {
			pushByteSize := GetPushBytes(op)
			startMin := int(pc + 1)
			if startMin >= codeLen {
				startMin = codeLen
			}
			endMin := startMin + pushByteSize
			if startMin+pushByteSize >= codeLen {
				endMin = codeLen
			}
			integer := new(uint256.Int)
			integer.SetBytes(contract.Code[startMin:endMin])
			if integer.IsUint64() {
				node.opValue = &AbsConst{Value, integer.Uint64()}
			} else {
				node.opValue = &AbsConst{Top, 0}
			}
			opLength += pushByteSize
		} else if op == JUMP {
			jumps = append(jumps, node)
		} else if op == JUMPI {
			jumps = append(jumps, node)
		} else if op == STOP || op == REVERT || op == RETURN || op == SELFDESTRUCT {

		}

		if lastNode != nil {
			lastNode.succs[node.pc] = node
		}

		pc += uint64(opLength)
		lastNode = node
	}

	for _, jump := range jumps {
		for _, node := range cfg.nodes {
			jump.succs[node.pc] = node
		}
	}

	for _, node := range cfg.nodes {
		for _, succ := range node.succs {
			succ.preds[node.pc] = node
		}
	}

	return cfg, nil
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

//////////////////////////////////////////////////
type AbsState struct {
	stack []AbsConst
}

func (state0 *AbsState) String() string {
	var stackStr []string
	for _, c := range state0.stack {
		stackStr = append(stackStr, c.String())
	}
	return fmt.Sprintf("%v", stackStr)
}

func (state0 *AbsState) Copy() *AbsState {
	state1 := &AbsState{}
	state1.stack = append([]AbsConst(nil), state0.stack...)
	return state1
}

func (state0 *AbsState) Push(value AbsConst) {
	state0.stack = append([]AbsConst{value}, state0.stack...)
}

func (state0 *AbsState) Pop() {
	state0.stack = state0.stack[1:]
}

func (state0 *AbsState) Canonicalize() {
	st := state0.stack
	for len(st) > 0 && st[len(st)-1].kind == Bot {
		st = st[:len(st)-1]
	}
	state0.stack = st
}

func (state0 *AbsState) Lub (state1 *AbsState) *AbsState {
	res := &AbsState{}

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

func (state0 *AbsState) Eq(state1 *AbsState) bool {
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

func Transfer(instrNode *Node, state0 *AbsState) *AbsState {
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

func DataFlowHarness(contract *Contract) {
	dataFlow, err := RunDataFlow(contract)
	if err != nil {
		panic(err)
	}

	dataFlow.Print()
}

type DataFlow struct {
	cfg *Cfg
	states map[uint64]*AbsState
}

func NewDataFlow(cfg *Cfg) *DataFlow {
	states := make(map[uint64]*AbsState)
	dataFlow := DataFlow{cfg, states}
	return &dataFlow
}

func (df *DataFlow) Print() {
	keys := make([]uint64, 0)
	for k, _ := range df.states {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, k := range keys {
		state := df.states[k]
		fmt.Printf("%-6d %-6v\n", k, state.String())
	}
}

func RunDataFlow(contract *Contract) (*DataFlow, error) {
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
		return nil, err
	}

	df := NewDataFlow(cfg0)
	workList := []*Node{cfg0.nodes[0]}

	for len(workList) > 0 {
		instrNode := workList[0]
		workList = workList[1:]

		prevLub := &AbsState{}
		for _, pred := range instrNode.preds {
			predState, exists := df.states[pred.pc]
			if !exists {
				predState = &AbsState{}
			}
			prevLub = prevLub.Lub(predState)
		}

		state0, exists := df.states[instrNode.pc]
		if !exists {
			state0 = &AbsState{}
		}
		state1 := Transfer(instrNode, prevLub)
		fmt.Println("xfr")
		fmt.Println(prevLub.String())
		fmt.Println(state1.String())
		df.states[instrNode.pc] = state1

		if !state0.Eq(state1) {
			for _, s := range instrNode.succs {
				workList = append(workList, s)
			}
		}
	}

	return df, nil
}


func GetPushBytes(op OpCode) int {
	switch op {
	case PUSH1:
		return 1
	case PUSH2:
		return 2
	case PUSH3:
		return 3
	case PUSH4:
		return 4
	case PUSH5:
		return 5
	case PUSH6:
		return 6
	case PUSH7:
		return 7
	case PUSH8:
		return 8
	case PUSH9:
		return 9
	case PUSH10:
		return 10
	case PUSH11:
		return 11
	case PUSH12:
		return 12
	case PUSH13:
		return 13
	case PUSH14:
		return 14
	case PUSH15:
		return 15
	case PUSH16:
		return 16
	case PUSH17:
		return 17
	case PUSH18:
		return 18
	case PUSH19:
		return 19
	case PUSH20:
		return 20
	case PUSH21:
		return 21
	case PUSH22:
		return 22
	case PUSH23:
		return 23
	case PUSH24:
		return 24
	case PUSH25:
		return 25
	case PUSH26:
		return 26
	case PUSH27:
		return 27
	case PUSH28:
		return 28
	case PUSH29:
		return 29
	case PUSH30:
		return 30
	case PUSH31:
		return 31
	case PUSH32:
		return 32
	default:
		panic("Cannot handle")
	}
}

// DfInterpreter is for static analysis
type DataFlowAnalysis struct {

}