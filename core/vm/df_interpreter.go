package vm

import (
	"github.com/ledgerwatch/turbo-geth/common"
)

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

//////////////////////////////////////////////////
type Cfg struct {
	nodes map[uint64]Node
}

type Node struct {
	pc uint64
	succs []Node
	preds []Node
}

func toCfg0(contract *Contract) (cfg *Cfg) {
	cfg = &Cfg{}

	op = contract.GetOp(pc)
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

func (lhs *AbsConst) lub(rhs AbsConst) AbsConst {
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

func (state0 *AbsState) canonicalize() {
	st := state0.stack
	for len(st) > 0 && st[len(st)-1].kind == Bot {
		st = st[:len(st)-1]
	}
	state0.stack = st
}

func (state0 *AbsState) lub (rhs *AbsState) *AbsState {
	res := &AbsState{}

	if len(state0.stack) > len(rhs.stack) {
		lhs0 := state0
		state0 = rhs
		rhs = lhs0
	}

	for i := 0; i < len(state0.stack); i++ {
		lub := state0.stack[i].lub(rhs.stack[i])
		res.stack = append(res.stack, lub)
	}

	for i := len(state0.stack); i < len(rhs.stack); i++ {
		res.stack = append(res.stack, rhs.stack[i])
	}

	res.canonicalize()

	return res
}

func (state0 *AbsState) isDiff(state1 *AbsState) bool {
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

func transfer(instrNode *Node, state0 *AbsState) *AbsState {
	res := &AbsState{}

	return res
}

func (in *DfInterpreter) Run(contract *Contract, input []byte, readOnly bool) {
	cfg0 := toCfg0(contract)
	states := make(map[uint64]*AbsState)
	workList := []Node{cfg0.nodes[0]}

	for len(workList) > 0 {
		instrNode := workList[0]
		workList = workList[1:]

		prevLub := &AbsState{}
		for _, pred := range instrNode.preds {
			predState, exists := states[pred.pc]
			if !exists {
				predState = &AbsState{}
			}
			prevLub = prevLub.lub(predState)
		}

		state0 := states[instrNode.pc]
		state1 := transfer(&instrNode, prevLub)
		states[instrNode.pc] = state1
		if state0.isDiff(state1) {
			for _, s := range instrNode.succs {
				workList = append(workList, s)
			}
		}
	}
}