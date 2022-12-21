package vm

import (
	"errors"
	"log"
	"reflect"

	"github.com/holiman/uint256"
)

type CfgOpSem struct {
	isPush   bool
	isDup    bool
	isSwap   bool
	numBytes int
	opNum    int
	numPush  int
	numPop   int
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
		opsem.isPush = op.isPush
		opsem.isDup = op.isDup
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
	if pc >= uint64(len(code)) {
		return false
	}

	return OpCode(code[pc]) == JUMPDEST
}

func resolveCheck(sem *CfgAbsSem, code []byte, st0 *astate, pc0 int) (map[int]bool, map[int]bool, error) {
	opcode := OpCode(code[pc0])
	opsem := (*sem)[opcode]
	succs := make(map[int]bool)
	jumps := make(map[int]bool)

	if opsem == nil {
		return succs, jumps, nil
	}

	codeLen := len(code)

	for _, stack := range st0.stackset {
		if opcode == JUMP || opcode == JUMPI {
			if stack.hasIndices(0) {
				jumpDest := stack.values[0]
				if jumpDest.kind == InvalidValue {
					//program terminates, don't add edges
				} else if jumpDest.kind == TopValue {
					empty := make(map[int]bool)
					return empty, empty, errors.New("unresolvable jumps found")
				} else if jumpDest.kind == ConcreteValue {
					if isJumpDest(code, jumpDest.value) {
						pc1 := int(jumpDest.value.Uint64())
						succs[pc1] = true
						jumps[pc1] = true
					}
				}
			}
		}
	}

	//fall-thru edge
	if opcode != JUMP {
		if pc0 < codeLen-opsem.numBytes {
			succs[pc0+opsem.numBytes] = true
		}
	}

	return succs, jumps, nil
}

func postCheck(sem *CfgAbsSem, code []byte, st0 *astate, pc0 int, pc1 int, isJump bool) *astate {
	st1 := emptyState()
	op0 := OpCode(code[pc0])
	opsem0 := (*sem)[op0]

	for _, stack0 := range st0.stackset {
		if isJump {
			if !stack0.hasIndices(0) {
				continue
			}

			elm0 := stack0.values[0]
			if elm0.kind == ConcreteValue && elm0.value.IsUint64() && int(elm0.value.Uint64()) != pc1 {
				continue
			}
		}

		stack1 := stack0.Copy()

		if opsem0.isPush {
			pushValue := getPushValue(code, pc0, opsem0)
			if isJumpDest(code, &pushValue) || isFF(&pushValue) {
				stack1.Push(AbsValueConcrete(pushValue))
			} else {
				stack1.Push(AbsValueInvalid())
			}
		} else if opsem0.isDup {
			if !stack0.hasIndices(opsem0.opNum - 1) {
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

		} else if op0 == AND {
			if !stack0.hasIndices(0, 1) {
				continue
			}

			a := stack1.Pop(pc0)
			b := stack1.Pop(pc0)

			if a.kind == ConcreteValue && b.kind == ConcreteValue {
				v := uint256.NewInt(0)
				v.And(a.value, b.value)
				stack1.Push(AbsValueConcrete(*v))
			} else {
				stack1.Push(AbsValueTop(pc0))
			}
		} else if op0 == PC {
			v := uint256.NewInt(0)
			v.SetUint64(uint64(pc0))
			stack1.Push(AbsValueConcrete(*v))
		} else {
			if !stack0.hasIndices(opsem0.numPop - 1) {
				continue
			}

			for i := 0; i < opsem0.numPop; i++ {
				stack1.Pop(pc0)
			}

			for i := 0; i < opsem0.numPush; i++ {
				stack1.Push(AbsValueTop(pc0))
			}
		}

		stack1.updateHash()
		st1.Add(stack1)
	}

	return st1
}

func CheckCfg(code []byte, proof *CfgProof) bool {
	sem := NewCfgAbsSem()

	if !proof.isValid() {
		return false
	}

	preLub := make(map[int][]*astate)
	for _, block := range proof.Blocks {
		st := intoAState(block.Entry.Stacks)
		pc0 := block.Entry.Pc
		blockSuccs := intMap(block.Succs)
		for pc0 <= block.Exit.Pc {

			if pc0 == block.Exit.Pc {
				if !Eq(st, intoAState(block.Exit.Stacks)) {
					return false
				}
			}

			succs, isJump, err := resolveCheck(sem, code, st, pc0)
			if err != nil {
				return false
			}

			if pc0 == block.Exit.Pc {
				if !reflect.DeepEqual(succs, blockSuccs) {
					return false
				}
				for succEntryPc := range succs {
					succEntrySt := postCheck(sem, code, st, pc0, succEntryPc, isJump[succEntryPc])
					preLub[succEntryPc] = append(preLub[succEntryPc], succEntrySt)
				}
				break
			} else {
				if len(succs) != 1 {
					return false
				}

				pc1 := one(succs)
				if pc0 >= pc1 || pc1 > block.Exit.Pc {
					return false
				}

				st = postCheck(sem, code, st, pc0, pc1, false)
				pc0 = pc1
			}
		}
	}

	for _, block := range proof.Blocks {

		var inferredEntry *astate
		if block.Entry.Pc == 0 {
			inferredEntry = botState()
		} else {
			lub := emptyState()
			for _, preSt := range preLub[block.Entry.Pc] {
				lub = Lub(lub, preSt)
			}
			inferredEntry = lub
		}

		if !Eq(inferredEntry, intoAState(block.Entry.Stacks)) {
			return false
		}
	}

	return true
}

func intMap(succs []int) map[int]bool {
	res := make(map[int]bool)
	for _, succ := range succs {
		res[succ] = true
	}
	return res
}

func one(m map[int]bool) int {
	for k := range m {
		return k
	}
	log.Fatal("must have exactly one element")
	return -1
}
