package vm

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/emicklei/dot"
	"github.com/holiman/uint256"
	"github.com/logrusorgru/aurora"
	"os"
	"sort"
	"strconv"
	"strings"
	"github.com/mitchellh/hashstructure"
)

//////////////////////////////////////////////////
type AbsValueKind int

//////////////////////////

// stmt is the representation of an executable instruction - extension of an opcode
type Astmt struct {
	inferredAsData bool
	ends           bool
	isBlockEntry   bool
	isBlockExit    bool
	covered        bool
	opcode         OpCode
	operation      *operation
	pc             int
	numBytes       int
	value          uint256.Int
	epilogue       bool
}

func (stmt *Astmt) String() string {
	ends := ""
	if stmt.ends {
		ends = "ends"
	}
	return fmt.Sprintf("%v %v %v", stmt.opcode, ends, stmt.operation.isPush)
}

type Program struct {
	Contract    *Contract
	Stmts       []*Astmt
	Blocks      []*Block
	Entry2block map[int]*Block
	Exit2block  map[int]*Block
}

func (program *Program) GetCodeHex() string {
	return hex.EncodeToString(program.Contract.Code)
}

func toProgram(contract *Contract) *Program {
	jt := newIstanbulInstructionSet()

	program := &Program{Contract: contract}

	codeLen := len(contract.Code)
	inferIsData := make(map[int]bool)
	for pc := 0; pc < codeLen; pc++ {
		stmt := Astmt{}
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

		program.Stmts = append(program.Stmts, &stmt)
		if pc != len(program.Stmts)-1 {
			panic("Invalid length")
		}
	}

	for pc, stmt := range program.Stmts {
		if !stmt.inferredAsData {
			if pc == 0 {
				stmt.isBlockEntry = true
			} else if stmt.opcode == JUMPDEST {
				stmt.isBlockEntry = true
			} else if stmt.opcode == JUMPI && pc < len(program.Stmts)-1 {
				entry := program.Stmts[pc+1]
				entry.isBlockEntry = true
			}

			if pc == len(program.Stmts)-1 {
				stmt.isBlockExit = true
			} else if stmt.opcode == JUMP || stmt.opcode == JUMPI {
				stmt.isBlockExit = true
			}
		}
	}

	program.Entry2block = make(map[int]*Block)
	program.Exit2block = make(map[int]*Block)

	for entrypc, entry := range program.Stmts {
		if entry.isBlockEntry {
			block := &Block{Entrypc: entrypc, Stmts: make([]*Astmt, 0)}
			program.Blocks = append(program.Blocks, block)
			for i := entrypc; i < len(program.Stmts); i++ {
				block.Stmts = append(block.Stmts, program.Stmts[i])
				if program.Stmts[i].isBlockExit {
					break
				}
			}

			if len(block.Stmts) > 0 {
				block.Exitpc = block.Stmts[len(block.Stmts)-1].pc
			}

			program.Entry2block[block.Entrypc] = block
			program.Exit2block[block.Exitpc] = block
		}
	}

	/*
	epilogue := []byte{0xa1, 0x65, 0x62, 0x7a, 0x7a, 0x72}

	for i := 0; i < len(program.Stmts) - len(epilogue) + 1; i++ {
		match := true
		for e := 0; e < len(epilogue); e++ {
			if byte(program.Stmts[i+e].opcode) != epilogue[e] {
				match = false
				break
			}
		}
		if match {
			for j := i; j < len(program.Stmts); j++ {
				program.Stmts[j].inferredAsData = true
				program.Stmts[j].epilogue = true
			}
			break
		}
	}*/

	return program
}

/*
func printStmts(stmts []*Astmt) {
	for i, stmt := range stmts {
		fmt.Printf("%v %v\n", i, stmt)
	}
}*/

////////////////////////

type edge struct {
	pc0    int
	stmt   *Astmt
	pc1    int
	isJump bool
}

func (e edge) String() string {
	return fmt.Sprintf("%v %v %v", e.pc0, e.pc1, e.stmt.opcode)
}

/*
func reverseSortEdges(edges []edge) {
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].pc0 > edges[j].pc0
	})
}

func sortEdges(edges []edge) {
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].pc0 < edges[j].pc0
	})
}*/

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
	kind          AbsValueKind
	value         uint256.Int //only when kind=ConcreteValue
	pc            int         //only when kind=TopValue
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

func AbsValueTop(pc int) AbsValue {
	return AbsValue{kind: TopValue, pc: pc}
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
	hash uint64
}

func newStack() *astack {
	st := &astack{}
	st.updateHash()
	return st
}

func (s *astack) Copy() *astack {
	newStack := &astack{}
	newStack.values = append(newStack.values, s.values...)
	newStack.hash = s.hash
	return newStack
}

func (s *astack) updateHash() {
	hash, err := hashstructure.Hash(s, nil)
	if err != nil {
		panic("cannot hash")
	}
	s.hash = hash
}

func (s *astack) Push(value AbsValue) {
	rest := s.values[:]
	s.values = nil
	s.values = append(s.values, value)
	s.values = append(s.values, rest...)
	s.updateHash()
}

func (s *astack) Pop(pc int) AbsValue {
	res := s.values[0]
	s.values = s.values[1:len(s.values)]
	//s.values = append(s.values, AbsValueTop(pc, true))
	s.updateHash()
	return res
}

func (s *astack) String(abbrev bool) string {
	strs := make([]string, 0)
	for _, c := range s.values {
		strs = append(strs, c.String(abbrev))
	}
	return strings.Join(strs, " ")
}

func (s *astack) Eq(s1 *astack) bool {
	if s.hash != s1.hash {
		return false
	}

	if len(s.values) != len(s1.values) {
		return false
	}

	for i := 0; i < len(s.values); i++ {
		if !s.values[i].Eq(s1.values[i]) {
			return false
		}
	}
	return true
}

func (s *astack) hasIndices(i ...int) bool {
	for _, i := range i {
		if !(i < len(s.values)) {
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

func botState() *astate {
	st := emptyState()

	botStack := newStack()
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
	maxStackLen := 0
	for _, stack := range state.stackset {
		if maxStackLen < len(stack.values) {
			maxStackLen = len(stack.values)
		}
	}

	var elms []string
	for i := 0; i < maxStackLen; i++ {
		var elm []string
		var values []AbsValue
		for _, stack := range state.stackset {
			if stack.hasIndices(i) {
				value := stack.values[i]
				if !ExistsIn(values, value) {
					elm = append(elm, value.String(abbrev))
					values = append(values, value)
				}
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

	elms = append(elms, fmt.Sprintf("%v%v%v", "|", len(state.stackset), "|"))
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
	edges    []edge
	resolved bool
}

// resolve analyses given executable instruction at given program counter in the context of given state
// It either concludes that the execution of the instruction may result in a jump to an unpredictable
// destination (in this case, attrubute resolved will be false), or returns one (for a non-JUMPI) or two (for JUMPI)
// edges that contain program counters of destinations where the executions can possibly come next
func resolve(cfg *Cfg, pc0 int) ([]edge, error) {
	isStackTooShort := false

	st0 := cfg.D[pc0]

	stmt := cfg.Program.Stmts[pc0]

	if stmt.ends {
		return nil, nil
	}

	codeLen := len(cfg.Program.Contract.Code)

	var edges []edge
	isBadJump := false

	//jump edges
	for _, stack := range st0.stackset {
		if stmt.opcode == JUMP || stmt.opcode == JUMPI {
			if stack.hasIndices(0) {
				jumpDest := stack.values[0]
				if jumpDest.kind == TopValue {
					isBadJump = true
					cfg.BadJumpImprecision = true
				} else if jumpDest.kind == ConcreteValue {
					if jumpDest.value.IsUint64() {
						pc1 := int(jumpDest.value.Uint64())

						if pc1 >= len(cfg.Program.Stmts) || cfg.Program.Stmts[pc1].operation == nil {
							//isBadJump = true
							//cfg.BadJumpInvalidOp = true
						} else if cfg.Program.Stmts[pc1].opcode != JUMPDEST {
							//isBadJump = true
							//cfg.BadJumpInvalidJumpDest = true
						} else {
							edges = append(edges, edge{pc0, stmt, pc1, true})
						}
					} else {
						//isBadJump = true
						//cfg.BadJumpInvalidJumpDest = true
					}
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

	for _, e := range edges {
		if cfg.PrevEdgeMap[e.pc1] == nil {
			cfg.PrevEdgeMap[e.pc1] = make(map[int]bool)
		}
		cfg.PrevEdgeMap[e.pc1][e.pc0] = true

		cfg.Program.Stmts[e.pc0].covered = true
		cfg.Program.Stmts[e.pc1].covered = true
	}

	if isStackTooShort {
		cfg.ShortStack = true
		return nil, errors.New("abstract stack too short: reached unmodelled depth")
	}

	if isBadJump {
		cfg.BadJumps[stmt.pc] = true
		cfg.Unresolved = true
		cfg.checkRep()
		return nil, errors.New("unresolvable jumps found")
	}

	edges = sortAndUnique(edges)

	cfg.checkRep()
	return edges, nil
}

type edgeKey struct {
	pc0 int
	pc1 int
}

func sortAndUnique(edges []edge) []edge {
	uedges := make([]edge, 0)

	edgeMap := make(map[edgeKey]bool)
	for i := 0; i < len(edges); i++ {
		e := edges[i]
		ek := edgeKey{e.pc0,e.pc1}
		if edgeMap[ek] {
			continue
		}

		edgeMap[ek] = true
		uedges = append(uedges, e)
	}

	sort.SliceStable(uedges, func(i, j int) bool {
		return uedges[i].pc0 < uedges[j].pc1
	})

	return uedges
}

func post(cfg *Cfg, st0 *astate, edge edge, maxStackLen int) (*astate, error) {
	st1 := emptyState()
	stmt := edge.stmt

	for _, stack0 := range st0.stackset {
		if edge.isJump {
			if !stack0.hasIndices(0) {
				continue
			}

			elm0 := stack0.values[0]
			if elm0.kind == ConcreteValue && elm0.value.IsUint64() && int(elm0.value.Uint64()) != edge.pc1 {
				continue
			}
		}

		stack1 := stack0.Copy()

		if stmt.opcode.IsPush() {
			stack1.Push(AbsValueConcrete(stmt.value))
		} else if stmt.operation.isDup {
			if !stack0.hasIndices(stmt.operation.opNum-1) {
				continue
			}

			value := stack1.values[stmt.operation.opNum-1]
			stack1.Push(value)
		} else if stmt.operation.isSwap {
			opNum := stmt.operation.opNum

			if !stack0.hasIndices(0, opNum) {
				continue
			}

			a := stack1.values[0]
			b := stack1.values[opNum]
			stack1.values[0] = b
			stack1.values[opNum] = a

		}  else if stmt.opcode == AND {
			if !stack0.hasIndices(0, 1) {
				continue
			}

			a := stack1.Pop(edge.pc0)
			b := stack1.Pop(edge.pc0)

			if a.kind == ConcreteValue && b.kind == ConcreteValue {
				v := uint256.NewInt()
				v.And(&a.value, &b.value)
				stack1.Push(AbsValueConcrete(*v))
			} else {
				stack1.Push(AbsValueTop(edge.pc0))
			}
		} else if stmt.opcode == PC {
			v := uint256.NewInt()
			v.SetUint64(uint64(stmt.pc))
			stack1.Push(AbsValueConcrete(*v))
		} else {
			if !stack0.hasIndices(stmt.operation.numPop-1) {
				continue
			}

			for i := 0; i < stmt.operation.numPop; i++ {
				stack1.Pop(edge.pc0)
			}

			for i := 0; i < stmt.operation.numPush; i++ {
				stack1.Push(AbsValueTop(edge.pc0))
			}
		}

		st1.Add(stack1)
	}

	for _, stack := range st1.stackset {
		if len(stack.values) > maxStackLen {
			cfg.ShortStack = true
			return nil, errors.New("Max stack length reached")
		}
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

type Block struct {
	Entrypc int
	Exitpc  int
	Stmts   []*Astmt
}


type Cfg struct {
	Valid            bool
	Panic            bool
	Unresolved       bool
	ShortStack       bool
	AnlyCounterLimit bool
	AnlyCounter      int
	Program          *Program
	BadJumps         map[int]bool
	PrevEdgeMap      map[int]map[int]bool
	D                map[int]*astate
	LowCoverage      bool

	BadJumpImprecision 		bool
	BadJumpInvalidOp   		bool
	BadJumpInvalidJumpDest 	bool

	StackCountLimitReached 	bool
}

type CfgCoverageStats struct {
	Covered				int
	Uncovered			int
	Instructions		int
	Coverage            int
	Epilogue		    int
}

func (cfg *Cfg) checkRep() {
	if true {
		return
	}

	for pc1, pc0s := range cfg.PrevEdgeMap {
		for pc0 := range pc0s {
			s := cfg.Program.Stmts[pc0]
			if s.ends {
				msg := fmt.Sprintf("Halt has successor: %v -> %v", pc0, pc1)
				panic(msg)
			}
		}
	}
}


func (cfg *Cfg) GetCoverageStats() CfgCoverageStats {
	stats := CfgCoverageStats{}
	firstUncovered := -1
	for pc, stmt := range cfg.Program.Stmts {
		if !stmt.inferredAsData {
			if stmt.covered {
				stats.Covered++
			} else {
				if firstUncovered < 0 {
					firstUncovered = pc
				}
			}
			stats.Instructions++
		}
	}

	stats.Epilogue = 0
	for i := len(cfg.Program.Stmts) - 1; i >= 0; i-- {
		stmt := cfg.Program.Stmts[i]
		if !stmt.inferredAsData {
			if cfg.Program.Stmts[i].covered {
				break
			}
			stats.Epilogue++
		}
	}

	stats.Uncovered = stats.Instructions - stats.Covered
	stats.Coverage = stats.Covered * 100 / stats.Instructions
	return stats
}

func (cfg *Cfg) PrintAnlyState() {
	//	es := make([]edge, len(edges))
	//	copy(es, edges)
	//	sortEdges(es)

	var badJumpList []string
	for pc, stmt := range cfg.Program.Stmts {
		if stmt.inferredAsData {
			//fmt.Printf("data: %v\n", stmt.inferredAsData)
			continue
		}

		if stmt.opcode == JUMPDEST {
			fmt.Println()
		}

		var valueStr string
		if stmt.opcode.IsPush() {
			valueStr = fmt.Sprintf("%v %v", stmt.opcode, stmt.value.Hex())
		} else {
			valueStr = fmt.Sprintf("%v", stmt.opcode)
		}

		pc0s := make([]string, 0)
		for pc0 := range cfg.PrevEdgeMap[pc] {
			pc0s = append(pc0s, strconv.Itoa(pc0))
		}

		if cfg.BadJumps[pc] {
			out := fmt.Sprintf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", aurora.Blue(cfg.D[pc].anlyCounter), aurora.Cyan(cfg.D[pc].worklistLen), aurora.Yellow(pc), aurora.Red(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), cfg.D[pc].String(false))
			fmt.Print(out)
			badJumpList = append(badJumpList, out)
		} else if cfg.PrevEdgeMap[pc] != nil {
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", aurora.Blue(cfg.D[pc].anlyCounter), aurora.Cyan(cfg.D[pc].worklistLen), aurora.Yellow(pc), aurora.Green(valueStr), aurora.Magenta(strings.Join(pc0s, ",")), cfg.D[pc].String(true))
		} else {
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v\n", aurora.Blue(cfg.D[pc].anlyCounter), aurora.Cyan(cfg.D[pc].worklistLen), aurora.Yellow(pc), valueStr)
		}
	}

	print("\nFull list of bad jumps:\n")
	for _, badJump := range badJumpList {
		fmt.Println(badJump)
	}

	g := dot.NewGraph(dot.Directed)
	block2node := make(map[*Block]*dot.Node)
	for _, block := range cfg.Program.Blocks {
		n := g.Node(fmt.Sprintf("%v\n%v", block.Entrypc, block.Exitpc)).Box()
		block2node[block] = &n
	}

	for pc1 := range cfg.PrevEdgeMap {
		for pc0 := range cfg.PrevEdgeMap[pc1] {
			block1 := cfg.Program.Entry2block[pc1]

			if block1 == nil {
				continue
			}

			block0 := cfg.Program.Exit2block[pc0]
			if block0 == nil {
				continue
			}

			n0 := block2node[block0]
			n1 := block2node[block1]
			g.Edge(*n0, *n1)
		}
	}

	path := "cfg.dot"
	_ = os.Remove(path)

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
	_ = w.Flush()
}

func (cfg *Cfg) GetBadJumpReason() string {
	if cfg.Valid {
		return ""
	}

	if cfg.ShortStack {
		return "ShortStack"
	}

	if cfg.AnlyCounterLimit {
		return "AnlyCounterLimit"
	}

	if cfg.BadJumpImprecision {
		return "Imprecision"
	}

	if cfg.BadJumpInvalidJumpDest {
		return "InvalidJumpDest"
	}

	if cfg.BadJumpInvalidOp {
		return "InvalidOpcode"
	}


	if cfg.Panic {
		return "Panic"
	}

	return "Unknown"
}

func pushNewEdges(workList []edge, edges []edge) []edge {
	for _, e := range edges {
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
	return workList
}


func GenCfg(contract *Contract, anlyCounterLimit int, maxStackLen int, maxStackCount int) (cfg *Cfg, err error) {
	/*defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New("internal panic")
			cfg.Panic = true
		}
	}()*/

	program := toProgram(contract)
	cfg = &Cfg{BadJumps: make(map[int]bool)}
	cfg.Program = program
	cfg.PrevEdgeMap = make(map[int]map[int]bool)

	startPC := 0
	codeLen := len(program.Contract.Code)
	cfg.D = make(map[int]*astate)
	for pc := 0; pc < codeLen; pc++ {
		cfg.D[pc] = emptyState()
	}
	cfg.D[startPC] = botState()

	var workList []edge

	edgesR1, errR1 := resolve(cfg, startPC)
	if errR1 != nil {
		return cfg, errR1
	}
	workList = pushNewEdges(workList, edgesR1)

	for len(workList) > 0 {
		if anlyCounterLimit > 0 && cfg.AnlyCounter > anlyCounterLimit {
			cfg.AnlyCounterLimit = true
			return cfg, errors.New("reached analysis counter limit")
		}

		var e edge
		e, workList = workList[0], workList[1:]

		preDpc0 := cfg.D[e.pc0]
		preDpc1 := cfg.D[e.pc1]

		post1, err := post(cfg, preDpc0, e, maxStackLen)
		if err != nil {
			return cfg, err
		}

		if !Leq(post1, preDpc1) {
			postDpc1 := Lub(post1, preDpc1)
			cfg.D[e.pc1] = postDpc1

			if len(postDpc1.stackset) > maxStackCount {
				/*fmt.Printf("stacklen: %v %v\n", len(postDpc1.stackset), maxStackCount)
				fmt.Println(postDpc1.String(false))
				for _, stack := range postDpc1.stackset {
					fmt.Printf("%v\n", stack.String(false))
				}*/
				cfg.StackCountLimitReached = true
				return cfg, errors.New("stack count limit reach")
			}

			edgesR2, errR2 := resolve(cfg, e.pc1)
			if errR2 != nil {
				return cfg, errR2
			}
			workList = pushNewEdges(workList, edgesR2)
		}

		decp1Copy := cfg.D[e.pc1]
		decp1Copy.anlyCounter = cfg.AnlyCounter
		decp1Copy.worklistLen = len(workList)
		cfg.D[e.pc1] = decp1Copy
		cfg.AnlyCounter++

		cfg.checkRep()
	}

	if len(cfg.BadJumps) > 0 {
		cfg.Unresolved = true
		return cfg, errors.New("unresolvable jumps found")
	}

	cov := cfg.GetCoverageStats()
	if cov.Uncovered > cov.Epilogue {
		cfg.LowCoverage = true
		//return cfg, errors.New("low coverage")
	}

	cfg.Valid = true
	return cfg, nil
}
