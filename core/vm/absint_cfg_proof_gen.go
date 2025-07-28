// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/erigontech/erigon-lib/common/dir"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/emicklei/dot"
	"github.com/holiman/uint256"
)

// ////////////////////////////////////////////////
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
}

func (stmt *Astmt) String() string {
	ends := ""
	if stmt.ends {
		ends = "ends"
	}
	return fmt.Sprintf("%v %v %v", stmt.opcode, ends, stmt.operation.isPush)
}

type Program struct {
	Code        []byte
	Stmts       []*Astmt
	Blocks      []*Block
	Entry2block map[int]*Block
	Exit2block  map[int]*Block
}

func (program *Program) GetCodeHex() string {
	return hex.EncodeToString(program.Code)
}

func (program *Program) isJumpDest(value *uint256.Int) bool {
	if !value.IsUint64() {
		return false
	}

	pc := value.Uint64()
	if pc >= uint64(len(program.Stmts)) {
		return false
	}

	stmt := program.Stmts[pc]
	if stmt == nil {
		return false
	}

	return stmt.opcode == JUMPDEST
}

func toProgram(code []byte) *Program {
	jt := newIstanbulInstructionSet()

	program := &Program{Code: code}

	codeLen := len(code)
	inferIsData := make(map[int]bool)
	for pc := 0; pc < codeLen; pc++ {
		stmt := Astmt{}
		stmt.pc = pc
		stmt.inferredAsData = inferIsData[pc]

		op := OpCode(code[pc])
		stmt.opcode = op
		stmt.operation = jt[op]
		stmt.ends = stmt.operation == nil
		//fmt.Printf("%v %v %v", pc, stmt.opcode, stmt.operation.valid)

		if op.IsPushWithImmediateArgs() {
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
			integer.SetBytes(code[startMin:endMin])
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

// resolve analyses given executable instruction at given program counter in the context of given state
// It either concludes that the execution of the instruction may result in a jump to an unpredictable
// destination (in this case, attrubute resolved will be false), or returns one (for a non-JUMPI) or two (for JUMPI)
// edges that contain program counters of destinations where the executions can possibly come next
func resolve(cfg *Cfg, pc0 int) ([]edge, error) {
	st0 := cfg.D[pc0]

	stmt := cfg.Program.Stmts[pc0]

	if stmt.ends {
		return nil, nil
	}

	codeLen := len(cfg.Program.Code)

	var edges []edge
	isBadJump := false

	//jump edges
	for _, stack := range st0.stackset {
		if stmt.opcode == JUMP || stmt.opcode == JUMPI {
			if stack.hasIndices(0) {
				jumpDest := stack.values[0]
				if jumpDest.kind == InvalidValue {
					//program terminates, don't add edges
				} else if jumpDest.kind == TopValue {
					isBadJump = true
					cfg.Metrics.BadJumpImprecision = true
				} else if jumpDest.kind == ConcreteValue {
					if cfg.Program.isJumpDest(jumpDest.value) {
						pc1 := int(jumpDest.value.Uint64())
						edges = append(edges, edge{pc0, stmt, pc1, true})
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

	if isBadJump {
		cfg.BadJumps[stmt.pc] = true
		cfg.Metrics.Unresolved = true
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
		ek := edgeKey{e.pc0, e.pc1}
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

		if stmt.opcode.IsPushWithImmediateArgs() {
			if cfg.Program.isJumpDest(&stmt.value) || isFF(&stmt.value) {
				stack1.Push(AbsValueConcrete(stmt.value))
			} else {
				stack1.Push(AbsValueInvalid())
			}
		} else if stmt.operation.isDup {
			if !stack0.hasIndices(stmt.operation.opNum - 1) {
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

		} else if stmt.opcode == AND {
			if !stack0.hasIndices(0, 1) {
				continue
			}

			a := stack1.Pop(edge.pc0)
			b := stack1.Pop(edge.pc0)

			if a.kind == ConcreteValue && b.kind == ConcreteValue {
				v := uint256.NewInt(0)
				v.And(a.value, b.value)
				stack1.Push(AbsValueConcrete(*v))
			} else {
				stack1.Push(AbsValueTop(edge.pc0))
			}
		} else if stmt.opcode == PC {
			v := uint256.NewInt(0)
			v.SetUint64(uint64(stmt.pc))
			stack1.Push(AbsValueConcrete(*v))
		} else {
			if !stack0.hasIndices(stmt.operation.numPop - 1) {
				continue
			}

			for i := 0; i < stmt.operation.numPop; i++ {
				stack1.Pop(edge.pc0)
			}

			for i := 0; i < stmt.operation.numPush; i++ {
				stack1.Push(AbsValueTop(edge.pc0))
			}
		}

		stack1.updateHash()
		st1.Add(stack1)
	}

	for _, stack := range st1.stackset {
		if len(stack.values) > maxStackLen {
			cfg.Metrics.ShortStack = true
			return nil, errors.New("max stack length reached")
		}
	}

	return st1, nil
}

func isFF(u *uint256.Int) bool {
	if u.IsUint64() && u.Uint64() == 4294967295 {
		return true
	}
	return false
}

type Block struct {
	Entrypc int
	Exitpc  int
	Stmts   []*Astmt
}

type CfgMetrics struct {
	Valid                  bool
	Panic                  bool
	Unresolved             bool
	ShortStack             bool
	AnlyCounterLimit       bool
	LowCoverage            bool
	BadJumpImprecision     bool
	BadJumpInvalidOp       bool
	BadJumpInvalidJumpDest bool
	StackCountLimitReached bool
	OOM                    bool
	Timeout                bool
	Checker                bool
	CheckerFailed          bool
	AnlyCounter            int
	NumStacks              int
	ProofSizeBytes         int
	Time                   time.Duration
	MemUsedMBs             uint64
}

type Cfg struct {
	Program         *Program
	BadJumps        map[int]bool
	PrevEdgeMap     map[int]map[int]bool
	D               map[int]*astate
	Metrics         *CfgMetrics
	ProofSerialized []byte
}

type CfgCoverageStats struct {
	Covered      int
	Uncovered    int
	Instructions int
	Coverage     int
	Epilogue     int
}

func (cfg *Cfg) Clear() {
	cfg.D = nil
	cfg.PrevEdgeMap = nil
	cfg.BadJumps = nil
	cfg.Program = nil
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
		if stmt.opcode.IsPushWithImmediateArgs() {
			valueStr = fmt.Sprintf("%v %v", stmt.opcode, stmt.value.Hex())
		} else {
			valueStr = fmt.Sprintf("%v", stmt.opcode)
		}

		pc0s := make([]string, 0)
		for pc0 := range cfg.PrevEdgeMap[pc] {
			pc0s = append(pc0s, strconv.Itoa(pc0))
		}

		if cfg.BadJumps[pc] {
			out := fmt.Sprintf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", cfg.D[pc].anlyCounter, cfg.D[pc].worklistLen, pc, valueStr, strings.Join(pc0s, ","), cfg.D[pc].String(false))
			fmt.Print(out)
			badJumpList = append(badJumpList, out)
		} else if cfg.PrevEdgeMap[pc] != nil {
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v %-10v %v\n", cfg.D[pc].anlyCounter, cfg.D[pc].worklistLen, pc, valueStr, strings.Join(pc0s, ","), cfg.D[pc].String(true))
		} else {
			fmt.Printf("[%5v] (w:%2v) %3v\t %-25v\n", cfg.D[pc].anlyCounter, cfg.D[pc].worklistLen, pc, valueStr)
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
	_ = dir.RemoveFile(path)

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

func (metrics *CfgMetrics) GetBadJumpReason() string {
	if metrics.Valid {
		return ""
	}

	if metrics.ShortStack {
		return "ShortStack"
	}

	if metrics.AnlyCounterLimit {
		return "AnlyCounterLimit"
	}

	if metrics.BadJumpImprecision {
		return "Imprecision"
	}

	if metrics.BadJumpInvalidJumpDest {
		return "InvalidJumpDest"
	}

	if metrics.BadJumpInvalidOp {
		return "InvalidOpcode"
	}

	if metrics.Panic {
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

func GenCfg(code []byte, anlyCounterLimit int, maxStackLen int, maxStackCount int, metrics *CfgMetrics) (cfg *Cfg, err error) {
	program := toProgram(code)
	cfg = &Cfg{Metrics: metrics}
	cfg.BadJumps = make(map[int]bool)
	cfg.Metrics = metrics
	cfg.Program = program
	cfg.PrevEdgeMap = make(map[int]map[int]bool)

	startPC := 0
	codeLen := len(program.Code)
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
		if anlyCounterLimit > 0 && cfg.Metrics.AnlyCounter > anlyCounterLimit {
			cfg.Metrics.AnlyCounterLimit = true
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
				//fmt.Printf("stacklen: %v %v\n", len(postDpc1.stackset), maxStackCount)
				//fmt.Println(postDpc1.String(false))
				//for _, stack := range postDpc1.stackset {
				//	fmt.Printf("%v\n", stack.String(false))
				//}
				cfg.Metrics.StackCountLimitReached = true
				return cfg, errors.New("stack count limit reach")
			}

			edgesR2, errR2 := resolve(cfg, e.pc1)
			if errR2 != nil {
				return cfg, errR2
			}
			workList = pushNewEdges(workList, edgesR2)
		}

		decp1Copy := cfg.D[e.pc1]
		decp1Copy.anlyCounter = cfg.Metrics.AnlyCounter
		decp1Copy.worklistLen = len(workList)
		cfg.D[e.pc1] = decp1Copy
		cfg.Metrics.AnlyCounter++

		cfg.checkRep()
	}

	if len(cfg.BadJumps) > 0 {
		cfg.Metrics.Unresolved = true
		return cfg, errors.New("unresolvable jumps found")
	}

	cov := cfg.GetCoverageStats()
	if cov.Uncovered > cov.Epilogue {
		cfg.Metrics.LowCoverage = true
		//return cfg, errors.New("low coverage")
	}

	cfg.Metrics.Valid = true

	return cfg, nil
}

func (cfg *Cfg) GenerateProof() *CfgProof {
	succEdgeMap := make(map[int][]int)
	proof := CfgProof{}
	entries := make(map[int]bool)
	exits := make(map[int]bool)

	pcs := make(map[int]bool)
	for pc1, pc0s := range cfg.PrevEdgeMap {
		for pc0 := range pc0s {
			succEdgeMap[pc0] = append(succEdgeMap[pc0], pc1)
			pcs[pc0] = true
			pcs[pc1] = true
		}
	}

	entries[0] = true
	for pc := range pcs {
		if pc == 0 || len(cfg.PrevEdgeMap[pc]) > 1 {
			entries[pc] = true
		}
		for pc0 := range cfg.PrevEdgeMap[pc] {
			if len(succEdgeMap[pc0]) > 1 {
				entries[pc] = true
				break
			}
		}
		opcode := OpCode(cfg.Program.Code[pc])
		if opcode == JUMPDEST {
			entries[pc] = true
		}
		if opcode == JUMPI {
			if pc < len(cfg.Program.Code)-1 {
				entries[pc+1] = true
			}
		}
	}

	for pc0 := range pcs {
		for _, pc1 := range succEdgeMap[pc0] {
			if entries[pc1] {
				exits[pc0] = true
				break
			}
		}

		if len(succEdgeMap[pc0]) == 0 || len(succEdgeMap[pc0]) > 1 {
			exits[pc0] = true
		}

		opcode := OpCode(cfg.Program.Code[pc0])
		if opcode == JUMP || opcode == JUMPI ||
			opcode == RETURN || opcode == REVERT {
			exits[pc0] = true
		}
	}

	entriesList := make([]int, 0)
	for pc := range entries {
		entriesList = append(entriesList, pc)
	}
	sort.Ints(entriesList)
	for _, pc0 := range entriesList {
		pc1 := pc0
		for !exits[pc1] {
			if len(succEdgeMap[pc1]) != 1 {
				panic("Inconsistent successors")
			}
			pc1 = succEdgeMap[pc1][0]
		}

		block := CfgProofBlock{}
		block.Entry = &CfgProofState{pc0, StringifyAState(cfg.D[pc0])}
		block.Exit = &CfgProofState{pc1, StringifyAState(cfg.D[pc1])}
		proof.Blocks = append(proof.Blocks, &block)
	}

	for _, predBlock := range proof.Blocks {
		for _, succBlock := range proof.Blocks {
			if cfg.PrevEdgeMap[succBlock.Entry.Pc][predBlock.Exit.Pc] {
				predBlock.Succs = append(predBlock.Succs, succBlock.Entry.Pc)
				succBlock.Preds = append(succBlock.Preds, predBlock.Exit.Pc)
			}
		}
	}

	return &proof
}
