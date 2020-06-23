package vm

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"sort"
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
	succs NodeSet
	preds NodeSet
}

func NewNode(pc uint64) *Node {
	return &Node {pc, make(NodeSet), make(NodeSet)}
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
	for int(pc) < len(contract.Code) {
		op := contract.GetOp(uint64(pc))
		node := NewNode(pc)

		cfg.nodes[pc] = node
		cfg.pcList = append(cfg.pcList, pc)

		opLength := uint64(1)
		operation := &jt[op]
		if !operation.valid {
			return nil, &ErrInvalidOpCode{opcode: op}
		}

		if op.IsPush() {
			opLength += GetPushBytes(op)
		} else if op == JUMP {
			jumps = append(jumps, node)
		} else if op == JUMPI {
			jumps = append(jumps, node)
		} else if op == STOP || op == REVERT || op == RETURN || op == SELFDESTRUCT {

		}

		if lastNode != nil {
			lastNode.succs[node.pc] = node
		}

		pc += opLength
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
type AbsConstValueType string

const (
	Bot AbsConstValueType = "bot"
	Top = "top"
	Value = "value"
)

type AbsConst struct {
	kind AbsConstValueType
	value uint64
}

func (lhs *AbsConst) Lub(rhs AbsConst) AbsConst {
	if lhs.kind == Value && rhs.kind == Value {
		if lhs.value != rhs.value {
			return AbsConst{Top, 0}
		} else {
			return AbsConst{Value,  lhs.value}
		}
	} else if lhs.kind == Top || rhs.kind == Top {
		return AbsConst{Top, 0}
	} else if lhs.kind == Bot {
		return AbsConst{Bot, rhs.value}
	} else if rhs.kind == Bot {
		return AbsConst{Bot, lhs.value}
	} else {
		panic("Missing condition")
	}
}

//////////////////////////////////////////////////
type AbsState struct {
	stack []AbsConst
}

func (state0 *AbsState) Canonicalize() {
	st := state0.stack
	for len(st) > 0 && st[len(st)-1].kind == Bot {
		st = st[:len(st)-1]
	}
	state0.stack = st
}

func (state0 *AbsState) Lub (rhs *AbsState) *AbsState {
	res := &AbsState{}

	if len(state0.stack) > len(rhs.stack) {
		state0prev := state0
		state0 = rhs
		rhs = state0prev
	}

	for i := 0; i < len(state0.stack); i++ {
		lub := state0.stack[i].Lub(rhs.stack[i])
		res.stack = append(res.stack, lub)
	}

	for i := len(state0.stack); i < len(rhs.stack); i++ {
		res.stack = append(res.stack, rhs.stack[i])
	}

	res.Canonicalize()

	return res
}

func (state0 *AbsState) IsDiff(state1 *AbsState) bool {
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
	res := &AbsState{}

	return res
}

func (in *DfInterpreter) Run(contract *Contract, input []byte, readOnly bool) error {

	jt := newIstanbulInstructionSet()

	cfg0, err := ToCfg0(contract, &jt)
	if err != nil {
		return err
	}

	states := make(map[uint64]*AbsState)
	workList := []*Node{cfg0.nodes[0]}

	for len(workList) > 0 {
		instrNode := workList[0]
		workList = workList[1:]

		prevLub := &AbsState{}
		for _, pred := range instrNode.preds {
			predState, exists := states[pred.pc]
			if !exists {
				predState = &AbsState{}
			}
			prevLub = prevLub.Lub(predState)
		}

		state0 := states[instrNode.pc]
		state1 := Transfer(instrNode, prevLub)
		states[instrNode.pc] = state1
		if state0.IsDiff(state1) {
			for _, s := range instrNode.succs {
				workList = append(workList, s)
			}
		}
	}

	return nil
}





func GetPushBytes(op OpCode) uint64 {
	switch(op) {
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
type DfInterpreter struct {
	evm *EVM
	cfg Config

	jt *JumpTable // EVM instruction table

	hasher    keccakState // Keccak256 hasher instance shared across opcodes
	hasherBuf common.Hash // Keccak256 hasher result array shared across opcodes

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse
}