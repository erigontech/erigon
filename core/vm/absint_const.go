package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"sort"
	"strings"
)

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

func (state * state) Push(value AbsConst) {
	rest := state.stack[0:absStackLen-1]
	state.stack = []AbsConst{value}
	state.stack = append(state.stack, rest...)
}

func (state * state) Pop() AbsConst {
	res := state.stack[0]
	state.stack = append(state.stack[1:], ConstTop())
	return res
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
	operation operation
	value uint256.Int
	numBytes int
}

func (state state) String() string {
	var strs []string
	for _, c := range state.stack {
		strs = append(strs, c.String())
	}
	return strings.Join(strs, " ")
}

func (stmt stmt) String() string {
	halts := ""
	if stmt.operation.halts {
		halts = "halts"
	}
	valid := ""
	if stmt.operation.valid {
		valid = "valid"
	}
	return fmt.Sprintf("%v %v %v", stmt.opcode, halts, valid)
}

type edge struct {
	pc0 int
	stmt stmt
	pc1 int
}

func (e edge) String() string {
	return fmt.Sprintf("%v %v %v", e.pc0, e.pc1, e.stmt.opcode)
}

func resolve(prog *Contract, pc0 int, st0 state, stmt stmt) []edge {
	if !stmt.operation.valid || stmt.operation.halts {
		return nil
	}

	codeLen := len(prog.Code)
	var edges []edge
	if stmt.opcode == JUMP || stmt.opcode == JUMPI {
		jumpDest := st0.stack[0]
		if jumpDest.kind == Value {
			if jumpDest.value.IsUint64() {
				pc1 := int(jumpDest.value.Uint64())
				edges = append(edges, edge{pc0, stmt, pc1})
			} else {
				panic("Invalid program counter. Cannot resolve jump.")
			}
		} else if jumpDest.kind == Top {
			panic("Imprecise jump found. Cannot resolve jump.")
		}
	}

	if stmt.opcode != JUMP {
		if pc0 < codeLen-stmt.numBytes {
			edges = append(edges, edge{pc0, stmt, pc0 + stmt.numBytes})
		}
	}

	fmt.Printf("\nResolve: %v %v\n", pc0, stmt)
	printEdges(edges)

	return edges
}

func post(st0 state, stmt stmt) state {
	st1 := st0

	if stmt.opcode.IsPush() {
		st1.Push(ConstValue(stmt.value))
	} else {
		switch stmt.opcode {
		case JUMP:
			st1.Pop()
		case JUMPI:
			st1.Pop()
			st1.Pop()
		case MLOAD:
			st1.Pop()
			st1.Push(ConstTop())
		case LT:
			lhs := st1.Pop()
			rhs := st1.Pop()
			if lhs.kind == Value && rhs.kind == Value {
				res := lhs.value.Lt(&rhs.value)
				resi := uint256.NewInt()
				if res {
					resi.SetOne()
				} else {
					resi.Clear()
				}
				st1.Push(ConstValue(*resi))
			} else {
				st1.Push(ConstTop())
			}
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

func getStmts(prog *Contract) []stmt {
	jt := newIstanbulInstructionSet()

	codeLen := len(prog.Code)
	var stmts []stmt
	for pc := 0; pc < codeLen; pc++ {
		stmt := stmt{}

		op := prog.GetOp(uint64(pc))
		stmt.opcode = op
		stmt.operation = jt[op]
		//fmt.Printf("%v %v %v", pc, stmt.opcode, stmt.operation.valid)

		if op.IsPush() {
			pushByteSize := GetPushBytes(op)
			startMin := pc + 1
			if startMin >= codeLen {
				startMin = codeLen
			}
			endMin := startMin + pushByteSize
			if startMin+pushByteSize >= codeLen {
				endMin = codeLen
			}
			integer := new(uint256.Int)
			integer.SetBytes(prog.Code[startMin:endMin])
			stmt.value = *integer
			stmt.numBytes = pushByteSize + 1
		} else {
			stmt.numBytes = 1
		}

		stmts = append(stmts, stmt)
		if pc != len(stmts) - 1 {
			panic("Invalid length")
		}
	}
	return stmts
}

func printEdges(edges []edge) {
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].pc0 < edges[j].pc0
	})

	for _, edge := range edges {
		fmt.Printf("%v\n", edge)
	}
}

func printStmts(stmts []stmt) {
	for i, stmt := range stmts {
		fmt.Printf("%v %v\n", i, stmt)
	}
}

func getEntryReachableEdges(entry int, edges []edge) []edge {
	pc2edges := make(map[int][]edge)
	for _, e := range edges {
		l := pc2edges[e.pc0]
		l = append(l, e)
		pc2edges[e.pc0] = l
	}

	workList := []int{entry}
	visited := make(map[int]bool)
	visited[entry] = true
	for len(workList) > 0 {
		var pc int
		pc, workList = workList[0], workList[1:]

		for _, edge := range pc2edges[pc] {
			if !visited[edge.pc1] {
				visited[edge.pc1] = true
				workList = append(workList, edge.pc1)
			}
		}
	}

	var reachable []edge
	for pc, exists := range visited {
		if exists {
			reachable = append(reachable, pc2edges[pc]...)
		}
	}
	return reachable
}


func AbsIntCfgHarness(prog *Contract) error {
	startPC := 0
	codeLen := len(prog.Code)
	D := make(map[int]state)
	for pc := 0; pc < codeLen; pc++ {
		D[pc] = botSt()
	}
	D[startPC] = startSt()

	stmts := getStmts(prog)
	printStmts(stmts)

	workList := resolve(prog, startPC, D[startPC], stmts[startPC])
	for len(workList) > 0 {
		print("\n\n")
		fmt.Println("worklist:")
		printEdges(workList)

		var e edge
		e, workList = workList[0], workList[1:]
		fmt.Printf("pre pc=%v\t%v\n", e.pc0, D[e.pc0])
		post1 := post(D[e.pc0], e.stmt)
		fmt.Printf("post\t\t%v\n", post1)
		fmt.Printf("D\t\t%v\n", D[e.pc1])
		if !leq(post1, D[e.pc1]) {
			D[e.pc1] = lub(post1, D[e.pc1])
			fmt.Printf("lub pc=%v\t%v\n", e.pc1, D[e.pc1])
			workList = append(workList, resolve(prog, e.pc1, D[e.pc1], stmts[e.pc1])...)
		}
	}

	print("\nFinal resolve....")
	var edges []edge
	for pc := 0; pc < codeLen; pc++ {
		edges = append(edges, resolve(prog, pc, D[pc], stmts[pc])...)
	}
	//need to run a DFS from the entry point to pick only reachable stmts

	if edges != nil {
		edges = getEntryReachableEdges(0, edges)
	}

	fmt.Printf("\n# of edges: %v\n", len(edges))
	printEdges(edges)

	println("done")

	return nil
}
