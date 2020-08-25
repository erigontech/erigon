package vm

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/logrusorgru/aurora"
	"github.com/emicklei/dot"
	"os"
	"sort"
	"strconv"
	"strings"
)

//////////////////////////////////////////////////
type AbsValueKind int
const (
	absStackLen = 32
)

var DEBUG = false
var STOP_ON_ERROR = true


//////////////////////////

// stmt is the representation of an executable instruction - extension of an opcode
type stmt struct {
	pc             int
	opcode         OpCode
	operation      operation
	value          uint256.Int
	numBytes       int
	inferredAsData bool
	ends           bool
	isBlockEntry   bool
	isBlockExit    bool
}

func (stmt *stmt) String() string {
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

type program struct {
	contract* Contract
	stmts []*stmt
	blocks []*block
	entry2block map[int]*block
	exit2block map[int]*block
}

func toProgram(contract *Contract) *program {
	jt := newIstanbulInstructionSet()

	program := &program{contract: contract}

	codeLen := len(contract.Code)
	inferIsData := make(map[int]bool)
	for pc := 0; pc < codeLen; pc++ {
		stmt := stmt{}
		stmt.pc = pc
		stmt.inferredAsData = inferIsData[pc]

		op := contract.GetOp(uint64(pc))
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
			integer.SetBytes(contract.Code[startMin:endMin])
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

		program.stmts = append(program.stmts, &stmt)
		if pc != len(program.stmts) - 1 {
			panic("Invalid length")
		}
	}

	for pc, stmt := range program.stmts {
		if !stmt.inferredAsData {
			if pc == 0 {
				stmt.isBlockEntry = true
			} else if stmt.opcode == JUMPDEST {
				stmt.isBlockEntry = true
			} else if stmt.opcode == JUMPI && pc < len(program.stmts) - 1 {
				entry := program.stmts[pc+1]
				entry.isBlockEntry = true
			}

			if pc == len(program.stmts) - 1 {
				stmt.isBlockExit = true
			} else if stmt.opcode == JUMP || stmt.opcode == JUMPI {
				stmt.isBlockExit = true
			}
		}
	}

	program.entry2block = make(map[int]*block)
	program.exit2block = make(map[int]*block)

	for entrypc, entry := range program.stmts {
		if entry.isBlockEntry {
			block := &block{entrypc: entrypc, stmts: make([]*stmt, 0) }
			program.blocks = append(program.blocks, block)
			for i := entrypc; i < len(program.stmts); i++ {
				block.stmts = append(block.stmts, program.stmts[i])
				if program.stmts[i].isBlockExit {
					break
				}
			}

			if len(block.stmts) > 0 {
				block.exitpc = block.stmts[len(block.stmts)-1].pc
			}

			program.entry2block[block.entrypc] = block
			program.exit2block[block.exitpc] = block
		}
	}

	return program
}


func printStmts(stmts []stmt) {
	for i, stmt := range stmts {
		fmt.Printf("%v %v\n", i, stmt)
	}
}

////////////////////////

type edge struct {
	pc0 int
	stmt *stmt
	pc1 int
}

func (e edge) String() string {
	return fmt.Sprintf("%v %v %v", e.pc0, e.pc1, e.stmt.opcode)
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

////////////////////////


const (
	BotValue AbsValueKind = iota
	TopValue
	ConcreteValue
)


func (d AbsValueKind) String() string {
	return [...]string{"⊥", "⊤", "AbsValue"}[d]
}
//////////////////////////////////////////////////

type AbsValue struct {
	kind AbsValueKind
	value uint256.Int 	//only when kind=ConcreteValue
	pc int 				//only when kind=TopValue
}

func (c0 AbsValue) String(abbrev bool) string {
	if c0.kind == BotValue || c0.kind == TopValue {
		if !abbrev {
			return fmt.Sprintf("%v%v", c0.kind.String(), c0.pc)
		} else {
			return c0.kind.String()
		}
	} else {
		if c0.value.IsUint64() {
			return strconv.FormatUint(c0.value.Uint64(), 10)
		} else {
			return "256bit"
		}
	}
}

//////////////////////////////////////////////////

type ValueSet struct {
	values map[AbsValue]bool
	isTop bool
}

func (set ValueSet) Copy() ValueSet {
	if set.isTop {
		return ValueSet{isTop: true}
	} else {
		newSet := ValueSet{values: make(map[AbsValue]bool), isTop: false}
		for k, v := range set.values {
			newSet.values[k] = v
		}
		return newSet
	}
}

func (set ValueSet) String(abbrev bool) string {
	if set.isTop {
		return "⊤ˢ"
	}

	var strs []string
	hasTop := false
	for v, in := range set.values {
		if in {
			strs = append(strs, v.String(abbrev))
			if v.kind == TopValue {
				hasTop = true
			}
		}
	}

	res := strings.Join(strs, " ")
	if len(strs) == 0 {
		return "⊥"
	}

	if abbrev && hasTop {
		return "⊤"
	} else if len(strs) == 1 {
		return fmt.Sprintf("%v", res)
	} else {
		return fmt.Sprintf("{%v}", res)
	}
}

func ValueSetBot() ValueSet {
	return ValueSet{ values: make(map[AbsValue]bool), isTop: false }
}

func ValueSetTop() ValueSet {
	return ValueSet{ isTop: true }
}

func ValueSetLub(c0 ValueSet, c1 ValueSet) ValueSet {
	if c0.isTop || c1.isTop {
		return ValueSetTop()
	}

	res := ValueSet{ values: make(map[AbsValue]bool) }

	for k, v := range c0.values {
		res.values[k] = v
	}

	for k, v := range c1.values {
		res.values[k] = v
	}

	return res
}

func ValueSetLeq(c0 ValueSet, c1 ValueSet) bool {
	if c0.isTop && c1.isTop {
		return true
	} else if !c0.isTop && c1.isTop {
		return true
	} else if c0.isTop && !c1.isTop {
		return false
	} else if !c0.isTop && !c1.isTop {
		for k, v := range c0.values {
			if v && !c1.values[k] {
				return false
			}
		}
	} else {
		panic("unreachable")
	}
	return true
}

func ValueSetSingle(value uint256.Int, pc int) ValueSet {
	valueSet := ValueSet{values: make(map[AbsValue]bool)}
	valueSet.values[AbsValue{ConcreteValue, value, pc}] = true
	return valueSet
}

//////////////////////////////////////////////////

type state struct {
	stack       []ValueSet
	anlyCounter int
	worklistLen int
}

func emptyState() state {
	return state{stack: nil, anlyCounter: -1, worklistLen: -1}
}

func (state *state) Copy() state {
	newState := emptyState()
	for _, valueSet := range state.stack {
		newState.stack = append(newState.stack, valueSet.Copy())
	}
	return newState
}

func (state *state) Push(value ValueSet) {
	rest := state.stack[0: absStackLen-1]
	state.stack = []ValueSet{value}
	state.stack = append(state.stack, rest...)
}

func (state *state) Pop() ValueSet {
	res := state.stack[0]
	state.stack = append(state.stack[1:], ValueSetTop())
	return res
}

// botState generates initial state which is a stack of "bottom" values
func botState() state {
	st := emptyState()
	st.stack = make([]ValueSet, absStackLen)

	for i := range st.stack {
		st.stack[i] = ValueSetBot()
	}
	return st
}

func (state state) String(abbrev bool) string {
	var strs []string
	for _, c := range state.stack {
		strs = append(strs, c.String(abbrev))
	}
	return strings.Join(strs, " ")
}

//////////////////////////////////////////////////

type ResolveResult struct {
	edges []edge
	resolved bool
	badJump *stmt
}

// resolve analyses given executable instruction at given program counter in the context of given state
// It either concludes that the execution of the instruction may result in a jump to an unpredictable
// destination (in this case, attrubute resolved will be false), or returns one (for a non-JUMPI) or two (for JUMPI)
// edges that contain program counters of destinations where the executions can possibly come next
func resolve(program *program, pc0 int, st0 state) ResolveResult {
	stmt := program.stmts[pc0]

	if !stmt.operation.valid || stmt.ends {
		return ResolveResult{resolved: true}
	}

	codeLen := len(program.contract.Code)

	var edges []edge
	isBadJump := false

	if stmt.opcode == JUMP || stmt.opcode == JUMPI {
		jumpDestSet := st0.stack[0]
		if jumpDestSet.isTop {
			isBadJump = true
		} else {
			for jumpDest, _ := range jumpDestSet.values {
				if jumpDest.kind == ConcreteValue {
					if jumpDest.value.IsUint64() {
						pc1 := int(jumpDest.value.Uint64())

						if program.stmts[pc1].opcode != JUMPDEST {
							isBadJump = true
						} else {
							edges = append(edges, edge{pc0, stmt, pc1})
						}
					} else {
						isBadJump = true
					}
				} else if jumpDest.kind == TopValue {
					isBadJump = true
				}
			}
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

	if isBadJump {
		return ResolveResult{edges: edges, resolved: false, badJump: stmt}
	} else {
		return ResolveResult{edges: edges, resolved: true, badJump: nil}
	}
}

func post(st0 state, stmt *stmt) (state, error) {
	st1 := st0.Copy()

	isStackTooShort := false

	if stmt.opcode.IsPush() {
		valueSet := ValueSet{values: make(map[AbsValue]bool)}
		valueSet.values[AbsValue{kind: ConcreteValue, value: stmt.value, pc: stmt.pc}] = true
		st1.Push(valueSet)
	} else if stmt.operation.isDup {
		valueSet := st1.stack[stmt.operation.opNum-1]
		isStackTooShort = valueSet.isTop
		st1.Push(valueSet)
	} else if stmt.operation.isSwap {
		opNum := stmt.operation.opNum
		a := st1.stack[0]
		b := st1.stack[opNum]

		isStackTooShort =  a.isTop || b.isTop

		st1.stack[0] = b
		st1.stack[opNum] = a
	} else {
		for i := 0; i < stmt.operation.numPop; i++ {
			s := st1.Pop()
			isStackTooShort = isStackTooShort || s.isTop
		}
		for i := 0; i < stmt.operation.numPush; i++ {
			valueSet := ValueSet{values: make(map[AbsValue]bool)}
			valueSet.values[AbsValue{kind: TopValue, pc: stmt.pc}] = true
			st1.Push(valueSet)
		}
	}

	if isStackTooShort {
		return st1, errors.New("abstract stack too short: reached unmodelled depth")
	} else {
		return st1, nil
	}
}

func leq(st0 state, st1 state) bool {
	if len(st0.stack) != len(st1.stack) || absStackLen != len(st0.stack) {
		panic("mismatched stack len")
	}

	for i := 0; i < absStackLen; i++ {
		if !ValueSetLeq(st0.stack[i], st1.stack[i]) {
			return false
		}
	}
	return true
}

func lub(st0 state, st1 state) state {
	newState := emptyState()

	for i := 0; i < absStackLen; i++ {
		lub := ValueSetLub(st0.stack[i], st1.stack[i])
		newState.stack = append(newState.stack, lub)
	}

	return newState
}

type block struct {
	entrypc int
	exitpc  int
	stmts   []*stmt
}

func printAnlyState(program *program, prevEdgeMap map[int]map[int]bool, D map[int]state, badJumps map[int]bool) {
//	es := make([]edge, len(edges))
//	copy(es, edges)
//	sortEdges(es)

	var badJumpList []string
	for pc, stmt := range program.stmts {
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
			stmt0 := program.stmts[pc0]
			if pc0 + stmt0.numBytes != pc {
				showPC0s = true
			}
		}
		if !showPC0s && false {
			pc0s = nil
		}

		if badJumps[pc] {
			out := fmt.Sprintf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", 	aurora.Blue(D[pc].anlyCounter), aurora.Cyan(D[pc].worklistLen), aurora.Yellow(pc), aurora.Red(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), D[pc].String(false))
			fmt.Print(out)
			badJumpList = append(badJumpList, out)
		} else if prevEdgeMap[pc] != nil {
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", 			aurora.Blue(D[pc].anlyCounter), aurora.Cyan(D[pc].worklistLen), aurora.Yellow(pc), aurora.Green(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), D[pc].String(false))
		} else {
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v\n", 					aurora.Blue(D[pc].anlyCounter), aurora.Cyan(D[pc].worklistLen), aurora.Yellow(pc), valueStr)
		}
	}

	print("\nFull list of bad jumps:\n")
	for _, badJump := range badJumpList {
		fmt.Println(badJump)
	}

	g := dot.NewGraph(dot.Directed)
	block2node := make(map[*block]*dot.Node)
	for _, block := range program.blocks {
		n := g.Node(fmt.Sprintf("%v\n%v", block.entrypc, block.exitpc)).Box()
		block2node[block] = &n
	}

	for pc1, _ := range prevEdgeMap {
		for pc0, _ := range prevEdgeMap[pc1] {
			block1 := program.entry2block[pc1]

			if block1 == nil {
				continue
			}

			block0 := program.exit2block[pc0]
			if block0 == nil {
				continue
			}

			n0 := block2node[block0]
			n1 := block2node[block1]
			g.Edge(*n0, *n1)
		}
	}

	path := "cfg.dot"
	os.Remove(path)

	f, errcr := os.Create(path)
	if errcr != nil {
		panic(errcr)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, errwr := w.WriteString(g.String())
	if errwr != nil {
		panic(errwr)
	}
	w.Flush()
}

func check(program *program, prevEdgeMap map[int]map[int]bool) {

	for pc1, pc0s := range prevEdgeMap {
		for pc0 := range pc0s {
			s := program.stmts[pc0]
			if s.ends {
				msg := fmt.Sprintf("Halt has successor: %v -> %v", pc0, pc1)
				panic(msg)
			}
		}
	}
}

func AbsIntCfgHarness(contract *Contract) error {

	program := toProgram(contract)

	startPC := 0
	codeLen := len(program.contract.Code)
	D := make(map[int]state)
	for pc := 0; pc < codeLen; pc++ {
		D[pc] = botState()
	}
	D[startPC] = botState()

	prevEdgeMap := make(map[int]map[int]bool)

	var workList []edge
	{
		resolution := resolve(program, startPC, D[startPC])
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

	check(program, prevEdgeMap)

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
		post1, err := post(preDpc0, e.stmt)
		if err != nil {
			if STOP_ON_ERROR  {
				printAnlyState(program, prevEdgeMap, D, nil)
				fmt.Printf("FAILURE: pc=%v %v\n", e.pc0, err);
				return nil
			} else {
				fmt.Printf("FAILURE: pc=%v %v\n", e.pc0, err);
			}
		}

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

				/*
				for i := 0; i < absStackLen; i++ {
					c0 := post1.stack[i]
					c1 := preDpc1.stack[i]
					if !ValueSetLeq(c0, c1) {
						fmt.Printf("diff: \t\t\t%v %v %v %v %v\n", i, c0, c1, c0.kind, c1.kind)
						if c0.kind == ConstValueKind && c1.kind == ConstValueKind {
							fmt.Printf("\t\t\t\t\tEQ=%v\n", c0.value.Eq(&c1.value))
						}
					}
				}*/
				//fmt.Printf("lub\t\t\t%v\n", postDpc1)
				printAnlyState(program, prevEdgeMap, D, nil)
			}
			D[e.pc1] = postDpc1

			resolution := resolve(program, e.pc1, D[e.pc1])

			if !resolution.resolved {
				badJumps[resolution.badJump.pc] = true
				fmt.Printf("FAILURE: Unable to resolve: anlyCounter=%v pc=%x\n", aurora.Red(anlyCounter), aurora.Red(e.pc1))
				if STOP_ON_ERROR {
					badJumps := make(map[int]bool)
					badJumps[resolution.badJump.pc] = true
					printAnlyState(program, prevEdgeMap, D, badJumps)
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
		decp1Copy.worklistLen = len(workList)
		D[e.pc1] = decp1Copy
		anlyCounter++

		check(program, prevEdgeMap)
	}

	print("\nFinal resolve....")
	var finalEdges []edge
	for pc := 0; pc < codeLen; pc++ {
		resolution := resolve(program, pc, D[pc])
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

	printAnlyState(program, prevEdgeMap, D, nil)
	println("done valueset")

	if len(badJumps) > 0 {
		printAnlyState(program, prevEdgeMap, D, badJumps)
	}

	return nil
}