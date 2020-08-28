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
	absStackLen = 64
)

var DEBUG = false
var StopOnError = true


//////////////////////////

// stmt is the representation of an executable instruction - extension of an opcode
type stmt struct {
	pc             int
	opcode         OpCode
	operation      *operation
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
	return fmt.Sprintf("%v %v %v", stmt.opcode, ends)
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
		stmt.ends = stmt.operation == nil || stmt.operation.halts || stmt.operation.reverts
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
	isJump bool
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
	fromDeepStack bool	//only when Kind=TopValue
}

func (c0 AbsValue) String(abbrev bool) string {
	if c0.kind == BotValue {
		return c0.kind.String()
	} else if c0.kind == TopValue {
		if !abbrev {
			return fmt.Sprintf("%v%v", c0.kind.String(), c0.pc)
		}
		return c0.kind.String()
	} else if c0.value.IsUint64() {
		return strconv.FormatUint(c0.value.Uint64(), 10)
	}
	return "256bit"
}

func AbsValueTop(pc int, fromDeepStack bool) AbsValue {
	return AbsValue{kind: TopValue, pc: pc, fromDeepStack: fromDeepStack}
}

func AbsValueBot() AbsValue {
	return AbsValue{kind: BotValue}
}

func AbsValueConcrete(value uint256.Int) AbsValue {
	return AbsValue{kind: ConcreteValue, value: value}
}

func (c0 AbsValue) Eq(c1 AbsValue) bool {
	if c0.kind != c1.kind {
		return false
	}

	if c0.kind == ConcreteValue {
		if !c0.value.Eq(&c1.value) {
			return false
		}
	}

	return true
}

//////////////////////////////////////////////////
type astack struct {
	values []AbsValue
}

func (s *astack) Copy() *astack {
	newStack := &astack{}
	newStack.values = append(newStack.values, s.values...)
	return newStack
}

func (s *astack) Push(value AbsValue) {
	rest := s.values[0: absStackLen-1]
	s.values = nil
	s.values = append(s.values, value)
	s.values = append(s.values, rest...)
}

func (s *astack) Pop(pc int) AbsValue {
	res := s.values[0]
	s.values = s.values[1: absStackLen]
	s.values = append(s.values, AbsValueTop(pc, true))
	return res
}

func (s *astack) String(abbrev bool) string {
	var strs []string
	for _, c := range s.values {
		strs = append(strs, c.String(abbrev))
	}
	return strings.Join(strs, " ")
}

func (s *astack) Eq(s1 *astack) bool {
	for i := 0; i < absStackLen; i++ {
		if !s.values[i].Eq(s1.values[i]) {
			return false
		}
	}
	return true
}

//////////////////////////////////////////////////

type astate struct {
	stackset    []*astack
	anlyCounter int
	worklistLen int
}

func emptyState() *astate {
	return &astate{stackset: nil, anlyCounter: -1, worklistLen: -1}
}

func (state *astate) Copy() *astate {
	newState := emptyState()
	for _, stack := range state.stackset {
		newState.stackset = append(newState.stackset, stack.Copy())
	}
	return newState
}

// botState generates initial state which is a stack of "bottom" values
func botState() *astate {
	st := emptyState()

	botStack := &astack{}
	for i := 0; i < absStackLen; i++ {
		botStack.values = append(botStack.values, AbsValueBot())
	}
	st.stackset = append(st.stackset, botStack)

	return st
}

func ExistsIn(values []AbsValue, value AbsValue) bool {
	for _, v := range values {
		if value.Eq(v) {
			return true
		}
	}
	return false
}

func (state *astate) String(abbrev bool) string {
	var elms []string
	for i := 0; i < absStackLen; i++ {
		var elm []string
		var values []AbsValue
		for _, stack := range state.stackset {
			value := stack.values[i]
			if !ExistsIn(values, value) {
				elm = append(elm, value.String(abbrev))
				values = append(values, value)
			}
		}

		var e string
		if len(values) > 1 {
			e = fmt.Sprintf("{%v}", strings.Join(elm, ","))
		} else {
			e = fmt.Sprintf("%v", strings.Join(elm, ","))
		}
		elms = append(elms, e)
	}
	return strings.Join(elms, " ")
}

func (state *astate) Add(stack *astack) {
	for _, existing := range state.stackset {
		if existing.Eq(stack) {
			return
		}
	}
	state.stackset = append(state.stackset, stack)
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
func resolve(program *program, pc0 int, st0 *astate) ResolveResult {
	stmt := program.stmts[pc0]

	if stmt.ends {
		return ResolveResult{resolved: true}
	}

	codeLen := len(program.contract.Code)

	var edges []edge
	isBadJump := false

	//jump edges
	for _, stack := range st0.stackset {
		if stmt.opcode == JUMP || stmt.opcode == JUMPI {
			jumpDest := stack.values[0]
			if jumpDest.kind == TopValue {
				isBadJump = true
			} else if jumpDest.kind == ConcreteValue {
				if jumpDest.value.IsUint64() {
					pc1 := int(jumpDest.value.Uint64())

					if program.stmts[pc1].opcode != JUMPDEST {
						isBadJump = true
					} else {
						edges = append(edges, edge{pc0, stmt, pc1, true})
					}
				} else {
					isBadJump = true
				}
			}
		}
	}

	//fall-thru edge
	if stmt.opcode != JUMP {
		if pc0 < codeLen-stmt.numBytes {
			edges = append(edges, edge{pc0, stmt, pc0 + stmt.numBytes, false})
		}
	}

	if DEBUG {
		fmt.Printf("\nResolve: %v %v\n", pc0, stmt)
		printEdges(edges)
	}

	if isBadJump {
		return ResolveResult{edges: edges, resolved: false, badJump: stmt}
	}

	return ResolveResult{edges: edges, resolved: true, badJump: nil}
}

func post(st0 *astate, edge edge) (*astate, error) {
	st1 := emptyState()
	stmt := edge.stmt

	isStackTooShort := false

	for _, stack0 := range st0.stackset {
		elm0 := stack0.values[0]
		if edge.isJump {
			if elm0.kind == ConcreteValue && elm0.value.IsUint64() && int(elm0.value.Uint64()) != edge.pc1 {
				continue
			}
		}

		stack1 := stack0.Copy()

		if stmt.opcode.IsPush() {
			stack1.Push(AbsValueConcrete(stmt.value))
		} else if stmt.operation.isDup {
			value := stack1.values[stmt.operation.opNum-1]
			stack1.Push(value)

			isStackTooShort = isStackTooShort || value.fromDeepStack
		} else if stmt.operation.isSwap {
			opNum := stmt.operation.opNum
			a := stack1.values[0]
			b := stack1.values[opNum]
			stack1.values[0] = b
			stack1.values[opNum] = a

			isStackTooShort = isStackTooShort || a.fromDeepStack || b.fromDeepStack
		} else {
			for i := 0; i < stmt.operation.numPop; i++ {
				s := stack1.Pop(edge.pc0)
				isStackTooShort = isStackTooShort || s.fromDeepStack
			}

			for i := 0; i < stmt.operation.numPush; i++ {
				stack1.Push(AbsValueTop(edge.pc0, false))
			}
		}

		st1.Add(stack1)
	}

	if isStackTooShort {
		return st1, errors.New("abstract stack too short: reached unmodelled depth")
	}

	return st1, nil
}

func Leq(st0 *astate, st1 *astate) bool {
	for _, stack0 := range st0.stackset {
		var found bool
		for _, stack1 := range st1.stackset {
			if stack0.Eq(stack1) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func Lub(st0 *astate, st1 *astate) *astate {
	newState := emptyState()
	for _, stack := range st0.stackset {
		newState.Add(stack)
	}
	for _, stack := range st1.stackset {
		newState.Add(stack)
	}
	return newState
}

type block struct {
	entrypc int
	exitpc  int
	stmts   []*stmt
}

func printAnlyState(program *program, prevEdgeMap map[int]map[int]bool, D map[int]*astate, badJumps map[int]bool) {
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
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", 			aurora.Blue(D[pc].anlyCounter), aurora.Cyan(D[pc].worklistLen), aurora.Yellow(pc), aurora.Green(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), D[pc].String(true))
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

	for pc1 := range prevEdgeMap {
		for pc0 := range prevEdgeMap[pc1] {
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

func AbsIntCfgHarness(contract *Contract) {

	program := toProgram(contract)

	startPC := 0
	codeLen := len(program.contract.Code)
	D := make(map[int]*astate)
	for pc := 0; pc < codeLen; pc++ {
		D[pc] = emptyState()
	}
	D[startPC] = botState()

	prevEdgeMap := make(map[int]map[int]bool)

	var workList []edge
	{
		resolution := resolve(program, startPC, D[startPC])
		if !resolution.resolved {
			fmt.Printf("Unable to resolve at pc=%x\n", startPC)
			return
		}

		for _, e := range resolution.edges {
			if prevEdgeMap[e.pc1] == nil {
				prevEdgeMap[e.pc1] = make(map[int]bool)
			}
			prevEdgeMap[e.pc1][e.pc0] = true
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
		post1, err := post(preDpc0, e)
		if err != nil {
			if StopOnError  {
				printAnlyState(program, prevEdgeMap, D, nil)
				fmt.Printf("FAILURE: pc=%v %v\n", e.pc0, err);
				return
			}

			fmt.Printf("FAILURE: pc=%v %v\n", e.pc0, err);
		}

		if DEBUG {
			fmt.Printf("post\t\t%v\n", post1);
			fmt.Printf("Dprev\t\t%v\n", preDpc1)
		}

		if !Leq(post1, preDpc1) {
			postDpc1 := Lub(post1, preDpc1)
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
				if StopOnError {
					badJumps := make(map[int]bool)
					badJumps[resolution.badJump.pc] = true
					printAnlyState(program, prevEdgeMap, D, badJumps)
					return
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

	return
}