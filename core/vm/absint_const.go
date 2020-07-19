package vm

import "github.com/holiman/uint256"

//////////////////////////////////////////////////
const (
	absStackLen = 10
)

type AbsElmType int

const (
	//bot AbsElmType = iota
	//top
	value = iota
)

type state struct {
	kind AbsElmType
	stack []AbsConst
}

func botSt() state {
	st := state{value, make([]AbsConst, absStackLen)}
	for i := range st.stack {
		st.stack[i] = ConstBot()
	}
	return st
}

func startSt() state {
	return botSt()
}

type stmt struct {
	opcode OpCode
	value uint256.Int
}

type edge struct {
	pc0 int
	stmt stmt
	pc1 int
}

func resolve(prog *Contract, pc0 int, st0 state) []edge {
	return nil
}

func post(st0 state, stmt stmt) state {
	st1 := st0

	if stmt.opcode.IsPush() {
		st1.stack = append(st1.stack, AbsConst{value,stmt.value})
	} else {
		switch stmt.opcode {
		case JUMP, JUMPI:
			st1.stack = st1.stack[1:]
		}
	}

	return st1
}

func leq(st0 state, st1 state) bool {
	for i := 0; i < absStackLen; i++ {
		if !ConstLeq(st0.stack[i], st1.stack[i]) {
			return false
		}
	}
	return true
}

func lub(st0 state, st1 state) state {
	res := state{value, []AbsConst{}}

	for i := 0; i < absStackLen; i++ {
		lub := ConstLub(st0.stack[i], st1.stack[i])
		res.stack = append(res.stack, lub)
	}

	return res
}

func AbsIntCfgHarness(prog *Contract) {
	//jt := newIstanbulInstructionSet()

	startPC := 0
	codeLen := len(prog.Code)
	D := make(map[int]state)
	for pc := 0; pc < codeLen; pc++ {
		D[pc] = botSt()
	}
	D[startPC] = startSt()

	workList := resolve(prog, startPC, D[startPC])
	for len(workList) > 0 {
		var e edge
		e, workList = workList[0], workList[1:]
		post1 := post(D[e.pc0], e.stmt)
		if !leq(post1, D[e.pc1]) {
			D[e.pc1] = lub(post1, D[e.pc1])
			workList = append(workList, resolve(prog, e.pc1, D[e.pc1])...)
		}
	}

	var edges []edge
	for pc := 0; pc < codeLen; pc++ {
		edges = append(edges, resolve(prog, pc, D[pc])...)
	}
	println(edges)
	println("done")
}
