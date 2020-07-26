package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"sort"
)

type Cfg struct {
	pcList []uint64
	nodes map[uint64]*Node
	entry []*Node
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
	pc      uint64
	opCode  OpCode
	opValue *AbsConst
	succs   NodeSet
	preds   NodeSet
}

func NewNode(pc uint64, opCode OpCode) *Node {
	return &Node{pc, opCode, nil, make(NodeSet), make(NodeSet)}
}

func NewCfg() *Cfg {
	cfg := &Cfg{}
	cfg.nodes = make(map[uint64]*Node)
	cfg.entry = make([]*Node, 0)
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
			pushByteSize := operation.opNum
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
			node.opValue = &AbsConst{Value, *integer}
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

	cfg.entry = append(cfg.entry, cfg.nodes[0])

	return cfg, nil
}
