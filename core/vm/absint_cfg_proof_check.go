package vm

import (
	"errors"
	"github.com/holiman/uint256"
)

type CfgOpSem struct {
	reverts  bool
	halts    bool
	numBytes int
	isPush   bool
	isDup    bool
	isSwap   bool
	opNum    int
	numPush  int
	numPop 	 int
}

type CfgAbsSem map[OpCode]*CfgOpSem

func NewCfgAbsSem() *CfgAbsSem {
	jt := newIstanbulInstructionSet()

	sem := CfgAbsSem{}

	for opcode, op := range jt {
		if op == nil {
			continue
		}
		opsem := CfgOpSem{}
		opsem.reverts = op.reverts
		opsem.halts = op.halts
		opsem.isPush = op.isPush
		opsem.isDup =  op.isDup
		opsem.isSwap = op.isSwap
		opsem.opNum = op.opNum
		opsem.numPush = op.numPush
		opsem.numPop = op.numPop

		if opsem.isPush {
			opsem.numBytes = op.opNum + 1
		} else {
			opsem.numBytes = 1

		}
		sem[OpCode(opcode)] = &opsem
	}

	return &sem
}

func getPushValue(code []byte, pc int, opsem0 *CfgOpSem) uint256.Int {
	pushByteSize := opsem0.opNum
	startMin := pc + 1
	if startMin >= len(code) {
		startMin = len(code)
	}
	endMin := startMin + pushByteSize
	if startMin+pushByteSize >= len(code) {
		endMin = len(code)
	}
	integer := new(uint256.Int)
	integer.SetBytes(code[startMin:endMin])
	return *integer
}

func isJumpDest(code []byte, value *uint256.Int) bool {
	if !value.IsUint64() {
		return false
	}

	pc := value.Uint64()
	if pc < 0 || pc >= uint64(len(code)) {
		return false
	}

	return OpCode(code[pc]) == JUMPDEST
}

func CheckCfg(code []byte, proof *CfgProof) bool {
	return false
	/*sem := NewCfgAbsSem()

	for _, block := range proof.Blocks {
		pre := DestringifyAState(block.Entry.Stacks)

		edges, err := resolveCheck(sem, code, pre, block.Entry)
		if err != nil {
			return false
		}

		resolveCheck()
		post, err := postCheck(sem, code, pre, e)
	}


	var workList []edgec

	if len(proof[0].stackset) != 1 || len(proof[0].stackset[0].values) != 0 {
		return false
	}

	edges, err := resolveCheck(sem, code, proof, 0)
	if err != nil {
		return false
	}

	visited := make(map[int]bool)
	for _, e := range edges {
		workList = append(workList, e)
	}

	for len(workList) > 0 {
		var e edgec
		e, workList = workList[0], workList[1:]

		if visited[e.pc0] {
			continue
		}
		visited[e.pc0] = true

		prePrf := proof[e.pc0]
		postPrf := proof[e.pc1]
		post, err := postCheck(sem, code, prePrf, e)
		if err != nil {
			return false
		}

		if !Leq(post, postPrf) {
			return false
		}

		edges, err := resolveCheck(sem, code, proof, e.pc1)
		if err != nil {
			return false
		}

		for _, e := range edges {
			if !visited[e.pc1] {
				workList = append(workList, e)
			}
		}
	}*/
}

func resolveCheck(sem *CfgAbsSem, code []byte, proof map[int]*astate, pc0 int) ([]edgec, error) {
	st0 := proof[pc0]
	opcode := OpCode(code[pc0])
	opsem := (*sem)[opcode]

	if opsem == nil || opsem.halts || opsem.reverts {
		return nil, nil
	}

	codeLen := len(code)

	var edges []edgec

	for _, stack := range st0.stackset {
		if opcode == JUMP || opcode == JUMPI {
			if stack.hasIndices(0) {
				jumpDest := stack.values[0]
				if jumpDest.kind == InvalidValue {
					//program terminates, don't add edges
				} else if jumpDest.kind == TopValue {
					return nil, errors.New("unresolvable jumps found")
				} else if jumpDest.kind == ConcreteValue {
					if isJumpDest(code, jumpDest.value) {
						pc1 := int(jumpDest.value.Uint64())
						edges = append(edges, edgec{pc0, pc1})
					} else {
						//program terminates, don't add edges
					}
				}
			}
		}
	}

	//fall-thru edge
	if opcode != JUMP {
		if pc0 < codeLen-opsem.numBytes {
			edges = append(edges, edgec{pc0, pc0 + opsem.numBytes})
		}
	}

	return edges, nil
}

func postCheck(sem *CfgAbsSem, code []byte, st0 *astate, edge edgec) (*astate, error) {
	st1 := emptyState()
	op0 := OpCode(code[edge.pc0])
	op1 := OpCode(code[edge.pc1])
	opsem0 := (*sem)[op0]

	for _, stack0 := range st0.stackset {
		if op1 == JUMPDEST {
			if !stack0.hasIndices(0) {
				continue
			}

			elm0 := stack0.values[0]
			if elm0.kind == ConcreteValue && elm0.value.IsUint64() && int(elm0.value.Uint64()) != edge.pc1 {
				continue
			}
		}

		stack1 := stack0.Copy()

		if opsem0.isPush {
			pushValue := getPushValue(code, edge.pc0, opsem0)
			if isJumpDest(code, &pushValue) || isFF(&pushValue) {
				stack1.Push(AbsValueConcrete(pushValue))
			} else {
				stack1.Push(AbsValueInvalid())
			}
		} else if opsem0.isDup {
			if !stack0.hasIndices(opsem0.opNum-1) {
				continue
			}

			value := stack1.values[opsem0.opNum-1]
			stack1.Push(value)
		} else if opsem0.isSwap {
			opNum := opsem0.opNum

			if !stack0.hasIndices(0, opNum) {
				continue
			}

			a := stack1.values[0]
			b := stack1.values[opNum]
			stack1.values[0] = b
			stack1.values[opNum] = a

		}  else if op0 == AND {
			if !stack0.hasIndices(0, 1) {
				continue
			}

			a := stack1.Pop(edge.pc0)
			b := stack1.Pop(edge.pc0)

			if a.kind == ConcreteValue && b.kind == ConcreteValue {
				v := uint256.NewInt()
				v.And(a.value, b.value)
				stack1.Push(AbsValueConcrete(*v))
			} else {
				stack1.Push(AbsValueTop(edge.pc0))
			}
		} else if op0 == PC {
			v := uint256.NewInt()
			v.SetUint64(uint64(edge.pc0))
			stack1.Push(AbsValueConcrete(*v))
		} else {
			if !stack0.hasIndices(opsem0.numPop-1) {
				continue
			}

			for i := 0; i < opsem0.numPop; i++ {
				stack1.Pop(edge.pc0)
			}

			for i := 0; i < opsem0.numPush; i++ {
				stack1.Push(AbsValueTop(edge.pc0))
			}
		}

		st1.Add(stack1)
	}

	return st1, nil
}
