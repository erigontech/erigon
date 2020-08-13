package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"sort"
	"strconv"
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
	stateValueKind = iota
)

const (
	failOnBadJump = false
)

type state struct {
	kind AbsElmType
	stack []AbsConst
	anlyCounter int
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
	st := state{stateValueKind, make([]AbsConst, absStackLen), -1}
	for i := range st.stack {
		st.stack[i] = ConstBot()
	}
	return st
}

func startSt() state {
	return botSt()
}

type stmt struct {
	pc             int
	opcode         OpCode
	operation      operation
	value          uint256.Int
	numBytes       int
	inferredAsData bool
	ends           bool
}

func (state state) String() string {
	var strs []string
	for _, c := range state.stack {
		strs = append(strs, c.String())
	}
	return strings.Join(strs, " ")
}

func (stmt stmt) String() string {
	ends := ""
	if stmt.ends {
		ends = "ends"
	}
	valid := ""
	if stmt.operation.valid {
		valid = "valid"
	}
	return fmt.Sprintf("%v %v %v", stmt.opcode, ends, valid)
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
	if !stmt.operation.valid || stmt.ends {
		return ResolveResult{resolved: true}
	}

	codeLen := len(prog.Code)
	var edges []edge

	if stmt.opcode == JUMP || stmt.opcode == JUMPI {
		jumpDest := st0.stack[0]
		if jumpDest.kind == ConstValueKind {
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
	st1 := state{kind: stateValueKind}
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
	if len(st0.stack) != len(st1.stack) || absStackLen != len(st0.stack) {
		panic("mismatched stack len")
	}

	for i := 0; i < absStackLen; i++ {
		if !ConstLeq(st0.stack[i], st1.stack[i]) {
			return false
		}
	}
	return true
}

func lub(st0 state, st1 state) state {
	res := state{stateValueKind, []AbsConst{}, -1}

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
	inferIsData := make(map[int]bool)
	for pc := 0; pc < codeLen; pc++ {
		stmt := stmt{}
		stmt.pc = pc
		stmt.inferredAsData = inferIsData[pc]

		op := prog.GetOp(uint64(pc))
		stmt.opcode = op
		stmt.operation = jt[op]
		stmt.ends = stmt.operation.halts || stmt.operation.reverts || !stmt.operation.valid
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

			if !stmt.inferredAsData {
				for datapc := startMin; datapc < endMin; datapc++ {
					inferIsData[datapc] = true
				}
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

func printAnlyState(stmts []stmt, prevEdgeMap map[int]map[int]bool, D map[int]state, badJumps map[int]bool) {
//	es := make([]edge, len(edges))
//	copy(es, edges)
//	sortEdges(es)

	var badJumpList []string
	for pc, stmt := range stmts {
		if stmt.inferredAsData {
			//fmt.Printf("data: %v\n", stmt.inferredAsData)
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

		pc0s := make([]string, 0)
		showPC0s := false
		for pc0, _ := range prevEdgeMap[pc] {
			pc0s = append(pc0s, strconv.Itoa(pc0))
			stmt0 := stmts[pc0]
			if pc0 + stmt0.numBytes != pc {
				showPC0s = true
			}
		}
		if !showPC0s && false {
			pc0s = nil
		}

		if badJumps[pc] {
			out := fmt.Sprintf("[%5v] %3v\t %-25v %-10v %v\n", aurora.Blue(D[pc].anlyCounter), aurora.Yellow(pc), aurora.Red(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), D[pc])
			fmt.Print(out)
			badJumpList = append(badJumpList, out)
		} else if prevEdgeMap[pc] != nil {
			fmt.Printf("[%5v] %3v\t %-25v %-10v %v\n", aurora.Blue(D[pc].anlyCounter), aurora.Yellow(pc), aurora.Green(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), D[pc])
		} else {
			fmt.Printf("[%5v] %3v\t %-25v\n", aurora.Blue(D[pc].anlyCounter), aurora.Yellow(pc), valueStr)
		}
	}

	print("\nFull list of bad jumps:\n")
	for _, badJump := range badJumpList {
		fmt.Println(badJump)
	}
}

func check(stmts []stmt, prevEdgeMap map[int]map[int]bool) {

	for pc1, pc0s := range prevEdgeMap {
		for pc0 := range pc0s {
			s := stmts[pc0]
			if s.ends {
				msg := fmt.Sprintf("Halt has successor: %v -> %v", pc0, pc1)
				panic(msg)
			}
		}
	}
}

func AbsIntCfgHarness(prog *Contract) error {

	stmts := getStmts(prog)
	if DEBUG {
		printStmts(stmts)
	}

	startPC := 0
	codeLen := len(prog.Code)
	D := make(map[int]state)
	for pc := 0; pc < codeLen; pc++ {
		D[pc] = botSt()
	}
	D[startPC] = startSt()

	prevEdgeMap := make(map[int]map[int]bool)

	var workList []edge
	{
		resolution := resolve(prog, startPC, D[startPC], stmts[startPC])
		if !resolution.resolved {
			fmt.Printf("Unable to resolve at pc=%x\n", startPC)
			return nil
		} else {
			for _, e := range resolution.edges {
				if prevEdgeMap[e.pc1] == nil {
					prevEdgeMap[e.pc1] = make(map[int]bool)
				}
				prevEdgeMap[e.pc1][e.pc0] = true
			}
		}
		workList = resolution.edges
	}

	check(stmts, prevEdgeMap)

	anlyCounter := 0
	badJumps := make(map[int]bool)
	for len(workList) > 0 {
		//sortEdges(workList)
		var e edge
		e, workList = workList[0], workList[1:]

		//fmt.Printf("%v\n", e.pc0)
		if e.pc0 == -1 {
			fmt.Printf("---------------------------------------\n")
			fmt.Printf("Verbose debugging for pc=%v\n", e.pc0)
			DEBUG = true
		}

		if DEBUG {
			fmt.Printf("pre pc=%v\t%v\n", e.pc0, D[e.pc0])
		}
		preDpc0 := D[e.pc0]
		preDpc1 := D[e.pc1]
		post1 := post(preDpc0, e.stmt)
		if DEBUG {
			fmt.Printf("post\t\t%v\n", post1);
			fmt.Printf("Dprev\t\t%v\n", preDpc1)
		}

		if !leq(post1, preDpc1) {
			postDpc1 := lub(post1, preDpc1)
			if false {

				fmt.Printf("\nedge %v %v\n", e.pc0, e.pc1)
				//fmt.Printf("pre D[pc0]\t\t%v\n", preDpc0);
				fmt.Printf("pre D[pc1]\t\t%v\n", preDpc1)
				fmt.Printf("post\t\t\t%v\n", post1)

				for i := 0; i < absStackLen; i++ {
					c0 := post1.stack[i]
					c1 := preDpc1.stack[i]
					if !ConstLeq(c0, c1) {
						fmt.Printf("diff: \t\t\t%v %v %v %v %v\n", i, c0, c1, c0.kind, c1.kind)
						if c0.kind == ConstValueKind && c1.kind == ConstValueKind  {
							fmt.Printf("\t\t\t\t\tEQ=%v\n", c0.value.Eq(&c1.value))
						}
					}
				}
				//fmt.Printf("lub\t\t\t%v\n", postDpc1)
				printAnlyState(stmts, prevEdgeMap, D, nil)
			}
			D[e.pc1] = postDpc1

			resolution := resolve(prog, e.pc1, D[e.pc1], stmts[e.pc1])

			if !resolution.resolved {
				badJumps[resolution.badJump.pc] = true
				fmt.Printf("FAILURE: Unable to resolve: anlyCounter=%v pc=%x\n", aurora.Red(anlyCounter), aurora.Red(e.pc1))
				if failOnBadJump {
					badJumps := make(map[int]bool)
					badJumps[resolution.badJump.pc] = true
					printAnlyState(stmts, prevEdgeMap, D, badJumps)
					return nil
				}
			} else {
				for _, e := range resolution.edges {
					inWorkList := false
					for _, w := range workList {
						if w.pc0 == e.pc0 && w.pc1 == e.pc1 {
							inWorkList = true
						}
					}
					if !inWorkList {
						head := []edge{e}
						workList = append(head, workList...)
					}
				}

				if prevEdgeMap[e.pc1] == nil {
					prevEdgeMap[e.pc1] = make(map[int]bool)
				}
				prevEdgeMap[e.pc1][e.pc0] = true
			}
		}
		DEBUG = false

		decp1Copy := D[e.pc1]
		decp1Copy.anlyCounter = anlyCounter
		D[e.pc1] = decp1Copy
		anlyCounter++

		check(stmts, prevEdgeMap)
	}

	print("\nFinal resolve....")
	var finalEdges []edge
	for pc := 0; pc < codeLen; pc++ {
		resolution := resolve(prog, pc, D[pc], stmts[pc])
		if !resolution.resolved {
			badJumps[resolution.badJump.pc] = true
			fmt.Println("Bad jump found during final resolve.")
		}
		finalEdges = append(finalEdges, resolution.edges...)
	}
	//need to run a DFS from the entry point to pick only reachable stmts

	reachableEdges := getEntryReachableEdges(0, finalEdges)

	fmt.Printf("\n# of unreachable edges: %v\n", len(finalEdges) - len(reachableEdges))
	fmt.Printf("\n# of total edges: %v\n", len(finalEdges))
	//printEdges(edges)

	printAnlyState(stmts, prevEdgeMap, D, nil)
	println("done")

	if len(badJumps) > 0 {
		printAnlyState(stmts, prevEdgeMap, D, badJumps)
	}

	return nil
}