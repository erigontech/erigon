package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"sort"
	"strings"
	"github.com/logrusorgru/aurora"
)

//////////////////////////////////////////////////
const (
	absStackLen = 32
)

var DEBUG = false

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
	pc int
	opcode OpCode
	operation operation
	value uint256.Int
	numBytes int
	isData bool
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

type ResolveResult struct {
	edges []edge
	resolved bool
	badJump *stmt
}

func resolve(prog *Contract, pc0 int, st0 state, stmt stmt) ResolveResult {
	if !stmt.operation.valid || stmt.operation.halts {
		return ResolveResult{resolved: true}
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
				return ResolveResult{resolved: false, badJump: &stmt}
			}
		} else if jumpDest.kind == Top {
			return ResolveResult{resolved: false, badJump: &stmt}
		}
	}

	if stmt.opcode != JUMP {
		if pc0 < codeLen-stmt.numBytes {
			edges = append(edges, edge{pc0, stmt, pc0 + stmt.numBytes})
		}
	}

	if DEBUG {
		fmt.Printf("\nResolve: %v %v\n", pc0, stmt)
		printEdges(edges)
	}

	return ResolveResult{edges: edges, resolved: true}
}

func post(st0 state, stmt stmt) state {
	st1 := state{kind: value}
	st1.stack = make([]AbsConst, len(st0.stack))
	copy(st1.stack, st0.stack)

	if stmt.opcode.IsPush() {
		st1.Push(ConstValue(stmt.value))
	} else if stmt.operation.isDup {
		value := st1.stack[stmt.operation.opNum-1]
		st1.Push(value)
	} else if stmt.operation.isSwap {
		opNum := stmt.operation.opNum
		a := st1.stack[0]
		b := st1.stack[opNum]
		st1.stack[0] = b
		st1.stack[opNum] = a
	} else {
		for i := 0; i < stmt.operation.numPop; i++ {
			st1.Pop()
		}
		for i := 0; i < stmt.operation.numPush; i++ {
			st1.Push(ConstTop())
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
	isData := make(map[int]bool)
	for pc := 0; pc < codeLen; pc++ {
		stmt := stmt{}
		stmt.pc = pc
		stmt.isData = isData[pc]

		op := prog.GetOp(uint64(pc))
		stmt.opcode = op
		stmt.operation = jt[op]
		//fmt.Printf("%v %v %v", pc, stmt.opcode, stmt.operation.valid)

		if op.IsPush() {
			pushByteSize := stmt.operation.opNum
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
			for datapc := startMin; datapc < endMin; datapc++ {
				isData[datapc] = true
			}
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

func reverseSortEdges(edges []edge) {
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].pc0 > edges[j].pc0
	})
}

func sortEdges(edges []edge) {
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].pc0 < edges[j].pc0
	})
}

func printEdges(edges []edge) {
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

func printAnlyState(stmts []stmt, edges map[*edge]bool, D map[int]state, badJump *stmt) {
//	es := make([]edge, len(edges))
//	copy(es, edges)
//	sortEdges(es)

	prev := make(map[int]map[int]bool)
	covered := make(map[int]bool)
	for e, _ := range edges {
		covered[e.pc0] = true
		covered[e.pc1] = true
		if prev[e.pc1] == nil {
			prev[e.pc1] = make(map[int]bool)
		}
		prev[e.pc1][e.pc0] = true
	}

	for pc, stmt := range stmts {
		if stmt.isData {
			continue
		}

		if stmt.opcode == JUMPDEST {
			fmt.Println()
		}

		var valueStr string
		if stmt.opcode.IsPush(){
			valueStr = fmt.Sprintf("%v %v", stmt.opcode, stmt.value.Hex())
		} else {
			valueStr = fmt.Sprintf("%v", stmt.opcode)
		}

		prevPCStr := ""
		for prevPC, _ := range prev[pc] {
			prevStmt := stmts[prevPC]
			if prevPC + prevStmt.numBytes != pc {
				prevPCs := make([]int, 0)
				for prevPC0, _ := range prev[pc] {
					prevPCs = append(prevPCs, prevPC0)
				}
				prevPCStr = fmt.Sprintf("%v", prevPCs)
			}
		}

		if badJump != nil && badJump.pc == pc {
			fmt.Printf("%3v\t %-25v %-10v %v\n", aurora.Red(pc), aurora.Red(valueStr), prevPCStr, D[pc])
		} else if covered[pc] {
			fmt.Printf("%3v\t %-25v %-10v %v\n", aurora.Green(pc), aurora.Green(valueStr), prevPCStr, D[pc])
		} else {
			fmt.Printf("%3v\t %-25v\n", pc, valueStr)
		}
	}
}

func AbsIntCfgHarness(prog *Contract) error {

	stmts := getStmts(prog)
	if DEBUG { printStmts(stmts) }

	startPC := 0
	codeLen := len(prog.Code)
	D := make(map[int]state)
	for pc := 0; pc < codeLen; pc++ {
		D[pc] = botSt()
	}
	D[startPC] = startSt()

	i := 0

	allEdges := make(map[*edge]bool)

	resolution := resolve(prog, startPC, D[startPC], stmts[startPC])
	if !resolution.resolved {
		fmt.Printf("Unable to resolve at pc=%x\n", startPC)
		return nil
	} else {
		for _, e := range resolution.edges {
			allEdges[&e] = true
		}
	}

	workList := resolution.edges
	for len(workList) > 0 {
		//sortEdges(workList)
		var e edge
		e, workList = workList[0], workList[1:]

		if e.pc0 == 76 {
			fmt.Printf("---------------------------------------\n")
			fmt.Printf("Verbose debugging for pc=%v\n", e.pc0)
			DEBUG = true
		}

		if DEBUG { fmt.Printf("pre pc=%v\t%v\n", e.pc0, D[e.pc0]) }
		post1 := post(D[e.pc0], e.stmt)
		if DEBUG {
			fmt.Printf("post\t\t%v\n", post1);
			fmt.Printf("Dprev\t\t%v\n", D[e.pc1])
		}

		if !leq(post1, D[e.pc1]) {
			D[e.pc1] = lub(post1, D[e.pc1])
			if DEBUG {
				fmt.Printf("lub pc=%v\t%v\n", e.pc1, D[e.pc1])
			}
			resolution = resolve(prog, e.pc1, D[e.pc1], stmts[e.pc1])
			if !resolution.resolved {
				printAnlyState(stmts, allEdges, D, resolution.badJump)
				fmt.Printf("Unable to resolve at pc=%x\n", aurora.Magenta(e.pc1))
				return nil
			} else {
				for _, e := range resolution.edges {
					allEdges[&e] = true
				}
			}
			workList = append(workList, resolution.edges...)
		}

		DEBUG = false

		i++
	}

	print("\nFinal resolve....")
	var finalEdges []edge
	for pc := 0; pc < codeLen; pc++ {
		resolution = resolve(prog, pc, D[pc], stmts[pc])
		if !resolution.resolved {
			printAnlyState(stmts, nil, D, resolution.badJump)
			fmt.Printf("Unable to resolve at pc=%x\n", pc)
			return nil
		}
		finalEdges = append(finalEdges, resolution.edges...)
	}
	//need to run a DFS from the entry point to pick only reachable stmts

	reachableEdges := getEntryReachableEdges(0, finalEdges)

	fmt.Printf("\n# of unreachable edges: %v\n", len(finalEdges) - len(reachableEdges))
	fmt.Printf("\n# of total edges: %v\n", len(finalEdges))
	//printEdges(edges)

	printAnlyState(stmts, allEdges, D, nil)
	println("done")

	return nil
}