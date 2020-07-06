package vm

import (
	"fmt"
	"sort"
)

//////////////////////////////////////////////////
type AbsState interface {
}

type AbsStateSpec interface {
	Bot() AbsState
	Lub(state0 AbsState, state1 AbsState) AbsState
	Eq(state0 AbsState, state1 AbsState) bool
	Transfer(state0 AbsState, node *Node) AbsState
	String(state0 AbsState) string
}

type DataFlow struct {
	spec AbsStateSpec
	cfg *Cfg
	states map[uint64]AbsState
}

func NewDataFlow(cfg *Cfg, spec AbsStateSpec) *DataFlow {
	states := make(map[uint64]AbsState)
	dataFlow := DataFlow{spec, cfg, states}
	return &dataFlow
}

func (df *DataFlow) Print() {
	keys := make([]uint64, 0)
	for k := range df.states {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	df.cfg.Print()

	fmt.Printf("%-6s %-12s %-6s\n", "PC", "INSTR", "STATE")
	for _, k := range keys {
		state := df.states[k]
		node := df.cfg.nodes[k]
		valueStr := opCodeToString[node.opCode]
		if node.opValue != nil {
			valueStr += " " + node.opValue.String()
		}
		fmt.Printf("%-6d %-12s %-6v\n", k, valueStr, df.spec.String(state))
	}
}

func (df *DataFlow) Run() error {
	spec := df.spec
	workList := make([]*Node, 0)
	workList = append(workList, df.cfg.entry...)

	for len(workList) > 0 {
		instrNode := workList[0]
		workList = workList[1:]

		prevLub := df.spec.Bot()
		for _, pred := range instrNode.preds {
			predState, exists := df.states[pred.pc]
			if !exists {
				predState = df.spec.Bot()
			}
			prevLub = df.spec.Lub(prevLub, predState)
		}

		state0, exists := df.states[instrNode.pc]
		if !exists {
			state0 = df.spec.Bot()
		}
		state1 := df.spec.Transfer(prevLub, instrNode)
		df.states[instrNode.pc] = state1

		if !spec.Eq(state0, state1) {
			for _, s := range instrNode.succs {
				workList = append(workList, s)
			}
		}
	}

	return nil
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