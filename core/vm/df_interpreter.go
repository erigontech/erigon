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


// DfInterpreter is for static analysis
type DataFlowAnalysis struct {

}