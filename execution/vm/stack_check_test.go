// Copyright 2026 The Erigon Authors
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
	"bytes"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// TestStackBoundsCheckEquivalence proves the interpreter's single unsigned
// range check fires exactly when the two-comparison form would, and that
// stackBoundsErr reproduces the same error type and fields, over the whole
// reachable domain (and beyond: negative lengths, in case of stack
// corruption).
func TestStackBoundsCheckEquivalence(t *testing.T) {
	t.Parallel()
	for numPop := 0; numPop <= 20; numPop++ {
		for numPush := 0; numPush <= 20; numPush++ {
			op := &operation{numPop: numPop, maxStack: maxStack(numPop, numPush)}
			for sLen := -2; sLen <= 1200; sLen++ {
				var want error
				if sLen < op.numPop {
					want = &ErrStackUnderflow{stackLen: sLen, required: op.numPop}
				} else if sLen > op.maxStack {
					want = &ErrStackOverflow{stackLen: sLen, limit: op.maxStack}
				}
				fired := uint(sLen-op.numPop) > uint(op.maxStack-op.numPop)
				require.Equal(t, want != nil, fired,
					"numPop=%d maxStack=%d sLen=%d", op.numPop, op.maxStack, sLen)
				if fired {
					require.Equal(t, want, stackBoundsErr(sLen, op))
				}
			}
		}
	}
}

// TestStackBoundsInvariant pins the table invariant the range check relies on:
// every entry of every fork table (including EnableEIP-patched copies, which
// go through validateAndFillMaxStack) satisfies 0 <= numPop <= maxStack.
func TestStackBoundsInvariant(t *testing.T) {
	t.Parallel()
	tables := map[string]*JumpTable{
		"frontier":         &frontierInstructionSet,
		"homestead":        &homesteadInstructionSet,
		"tangerineWhistle": &tangerineWhistleInstructionSet,
		"spuriousDragon":   &spuriousDragonInstructionSet,
		"byzantium":        &byzantiumInstructionSet,
		"constantinople":   &constantinopleInstructionSet,
		"istanbul":         &istanbulInstructionSet,
		"berlin":           &berlinInstructionSet,
		"london":           &londonInstructionSet,
		"shanghai":         &shanghaiInstructionSet,
		"cancun":           &cancunInstructionSet,
		"prague":           &pragueInstructionSet,
		"osaka":            &osakaInstructionSet,
		"amsterdam":        &amsterdamInstructionSet,
	}
	for name, jt := range tables {
		for i, op := range jt {
			require.NotNilf(t, op, "%s[0x%02X] nil entry", name, i)
			require.GreaterOrEqualf(t, op.numPop, 0, "%s[0x%02X]", name, i)
			require.LessOrEqualf(t, op.numPop, op.maxStack, "%s[0x%02X]", name, i)
		}
	}
}

// TestFastPathMatchesJumpTables pins the constants hard-coded in the
// interpreter's fast path to every jump table it can run with.
func TestFastPathMatchesJumpTables(t *testing.T) {
	t.Parallel()
	// DUPs and PUSH3+ are closures, whose code pointers differ by inlining site,
	// so they are checked by behaviour instead of by function identity.
	tables := []*JumpTable{
		&frontierInstructionSet, &homesteadInstructionSet, &tangerineWhistleInstructionSet,
		&spuriousDragonInstructionSet, &byzantiumInstructionSet, &constantinopleInstructionSet,
		&istanbulInstructionSet, &berlinInstructionSet, &londonInstructionSet,
		&shanghaiInstructionSet, &cancunInstructionSet, &pragueInstructionSet,
		&osakaInstructionSet, &amsterdamInstructionSet,
	}
	for _, jt := range tables {
		for eip := range activators {
			cp := copyJumpTable(jt)
			require.NoError(t, EnableEIP(eip, cp))
			tables = append(tables, cp)
		}
	}
	for i, jt := range tables {
		for op, w := range fastPathOps {
			got := &jt[op]
			if w.execute != nil {
				require.Equal(t, reflect.ValueOf(w.execute).Pointer(), reflect.ValueOf(got.execute).Pointer(), "table %d %s execute", i, op)
			} else {
				scope := new(CallContext)
				for v := range uint64(16) {
					scope.Stack.pushRef().SetUint64(v)
				}
				if op.IsPushWithImmediateArgs() {
					// The immediate is 16, the top the check below wants.
					scope.Contract.Code = append(make([]byte, op-PUSH0), 16)
				}
				_, _, err := got.execute(0, nil, scope)
				require.NoError(t, err)
				require.Equal(t, 17, scope.Stack.len(), "table %d %s stack", i, op)
				require.Equal(t, uint64(16-w.numPop), scope.Stack.peek().Uint64(), "table %d %s top", i, op)
			}
			require.Equal(t, w.gas, got.constantGas, "table %d %s gas", i, op)
			require.Equal(t, w.numPop, got.numPop, "table %d %s numPop", i, op)
			require.Equal(t, w.numPush, got.numPush, "table %d %s numPush", i, op)
			if w.memorySize == nil {
				require.Nil(t, got.dynamicGas, "table %d %s dynamicGas", i, op)
				require.Nil(t, got.memorySize, "table %d %s memorySize", i, op)
				continue
			}
			// Memory that need not grow costs no dynamic gas.
			require.Equal(t, reflect.ValueOf(pureMemoryGascost).Pointer(), reflect.ValueOf(got.dynamicGas).Pointer(), "table %d %s dynamicGas", i, op)
			require.Equal(t, reflect.ValueOf(w.memorySize).Pointer(), reflect.ValueOf(got.memorySize).Pointer(), "table %d %s memorySize", i, op)
		}
	}
}

// fastPathWant is one fastPathOps entry, generated with the fast-path switch in vm_run_gen.go.
type fastPathWant struct {
	execute         executionFunc
	gas             uint64
	numPop, numPush int
	memorySize      memorySizeFunc
}

// TestRunIsGenerated fails when vm_run_gen.go or fast_path_gen_test.go are
// stale against execution/vm/vmgen.
func TestRunIsGenerated(t *testing.T) {
	// The test cache keys on the files this process reads, not on what go run reads.
	srcs, err := filepath.Glob("vmgen/*.go")
	require.NoError(t, err)
	require.NotEmpty(t, srcs)
	for _, src := range srcs {
		_, err := os.ReadFile(src)
		require.NoError(t, err)
	}
	out, err := exec.CommandContext(t.Context(), "go", "run", "./vmgen", "-check").CombinedOutput()
	require.NoError(t, err, string(out))
}

// TestRunHasNoJumpTable fails when Go compiles run's opcode switch to a jump
// table, which it does once the cases are dense enough. The indirect jump made
// the fast path slower than the compare tree.
func TestRunHasNoJumpTable(t *testing.T) {
	// The test binary has no symbol table; the package archive keeps it.
	pkg := filepath.Join(t.TempDir(), "vm.a")
	out, err := exec.CommandContext(t.Context(), "go", "build", "-o", pkg, ".").CombinedOutput()
	require.NoError(t, err, string(out))
	out, err = exec.CommandContext(t.Context(), "go", "tool", "objdump", "-s", `vm\.\(\*EVM\)\.run$`, pkg).Output()
	require.NoError(t, err)
	require.Contains(t, string(out), "vm_run_gen.go")
	tableJump := regexp.MustCompile(`(?m)\tJMP (0\(\w+\)\(\w+\*8\)|\(R\d+\))\s`)
	require.Empty(t, tableJump.FindString(string(out)), "run dispatches through a jump table")
}

// TestRunLoopHeadStoresNothing fails when Go spills run's loop-carried registers at
// the top of the loop, which every op then pays: a value live across a call in any
// case is spilled there unless vmgen saves it around that call.
func TestRunLoopHeadStoresNothing(t *testing.T) {
	src, err := os.ReadFile("vm_run_gen.go")
	require.NoError(t, err)
	head := slices.IndexFunc(strings.Split(string(src), "\n"), func(l string) bool {
		return strings.Contains(l, "pc >= uint64(len(contract.Code))")
	}) + 1
	require.Positive(t, head)
	pkg := filepath.Join(t.TempDir(), "vm.a")
	out, err := exec.CommandContext(t.Context(), "go", "build", "-o", pkg, ".").CombinedOutput()
	require.NoError(t, err, string(out))
	out, err = exec.CommandContext(t.Context(), "go", "tool", "objdump", "-s", `vm\.\(\*EVM\)\.run$`, pkg).Output()
	require.NoError(t, err)
	// The loop head is the first run of instructions from its line; the out-of-line
	// stop path comes from the same line further down.
	at := fmt.Sprintf("vm_run_gen.go:%d\t", head)
	lines := strings.Split(string(out), "\n")
	first := slices.IndexFunc(lines, func(l string) bool { return strings.Contains(l, at) })
	require.Positive(t, first)
	spill := regexp.MustCompile(`\tMOV\w*\s+[^,\s]+, -?\w*\(R?SP\)`)
	for _, l := range lines[first:] {
		if !strings.Contains(l, at) {
			break
		}
		require.False(t, spill.MatchString(l), "run spills at the top of its loop: %s", l)
	}
}

// TestRunEmptyCodeReturnsBeforeTraceChoice pins that Run returns for empty code
// before it asks the state whether to trace instructions, so an EVM without a
// state still runs empty code.
func TestRunEmptyCodeReturnsBeforeTraceChoice(t *testing.T) {
	defer func(v bool) { dbg.TraceInstructions = v }(dbg.TraceInstructions)
	dbg.TraceInstructions = true
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
	gas := mdgas.MdGas{Execution: 100}
	ret, left, _, err := evm.Run(*NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{}), gas, nil, false)
	require.NoError(t, err)
	require.Nil(t, ret)
	require.Equal(t, gas, left)
}

// TestRunMatchesRunTraced runs each program through run and through runTraced,
// which has no fast path, at every gas budget up to the program's full cost, so
// every fast-path body must match its jump-table op in result, gas and error.
// It also runs them through runHooked and runTraced with an opcode hook that
// skips the fast-path ops, which must see the same events.
// Programs end by returning their top four stack items. A failing op's gas is
// not in the measured cost, so the full budget is always run as well.
func TestRunMatchesRunTraced(t *testing.T) {
	t.Parallel()
	slowOps := tracing.NewOpcodeMask(byte(GAS), byte(MSIZE), byte(NOT), byte(OR), byte(CALL), byte(RETURN), byte(INVALID))
	runOnce := func(code []byte, gas uint64, fast, hooked bool) (string, uint64) {
		var events []string
		var cfg Config
		if hooked {
			cfg.Tracer = &tracing.Hooks{
				OnOpcodeV2: func(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, _ tracing.OpContext, _ []byte, depth int, err error) {
					events = append(events, fmt.Sprintf("%d:%v:%d:%+v:%d:%v", pc, OpCode(op), gas.Execution, cost, depth, err))
				},
				OnOpcodeMask: slowOps,
			}
		}
		// A fresh state per run: a shared one leaves the first run's cold accesses warm.
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
		evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, cfg)
		c := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
		c.Code = code
		f := evm.runTraced
		switch {
		case fast && hooked:
			f = evm.runHooked
		case fast:
			f = evm.run
		}
		ret, left, used, err := f(*c, mdgas.MdGas{Execution: gas}, nil, false, hooked, false)
		return fmt.Sprintf("ret=%x left=%d used=%+v err=%v events=%v", ret, left.Execution, used, err, events), gas - left.Execution
	}
	prog := func(parts ...any) []byte {
		var b []byte
		for _, p := range parts {
			switch p := p.(type) {
			case OpCode:
				b = append(b, byte(p))
			case int:
				b = append(b, byte(p))
			case []byte:
				b = append(b, p...)
			}
		}
		return append(b, byte(PUSH1), 0, byte(MSTORE), byte(PUSH1), 32, byte(MSTORE), byte(PUSH1), 64, byte(MSTORE),
			byte(PUSH1), 96, byte(MSTORE), byte(PUSH1), 128, byte(PUSH1), 0, byte(RETURN))
	}
	pushes := func(n int) []byte { return bytes.Repeat([]byte{byte(PUSH1), 1}, n) }
	programs := map[string][]byte{
		"arith":    prog(PUSH1, 7, PUSH1, 3, SUB, PUSH1, 5, MUL, PUSH1, 2, DIV, PUSH1, 9, LT, PUSH1, 1, GT, PUSH1, 0, EQ, ISZERO, PUSH2, 0xff, 0x0f, AND, PUSH1, 4, ADD, PUSH1, 0, ISZERO, PUSH1, 6, PUSH1, 6, EQ),
		"loop":     prog(PUSH1, 5, JUMPDEST, PUSH1, 1, SWAP1, SUB, DUP1, PUSH1, 2, JUMPI, PUSH1, 17, JUMP, INVALID, INVALID, INVALID, JUMPDEST, pushes(3)),
		"memory":   prog(PUSH1, 0xaa, PUSH1, 0, MSTORE, PUSH1, 0, MLOAD, PUSH1, 16, MLOAD, PUSH1, 32, MLOAD, PUSH1, 0xbb, PUSH1, 8, MSTORE, PUSH1, 33, MLOAD, PUSH1, 8, MLOAD),
		"memhuge":  prog(PUSH8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, MLOAD),
		"memover":  prog(PUSH9, 1, 0, 0, 0, 0, 0, 0, 0, 0, PUSH1, 1, SWAP1, MSTORE),
		"push1end": {byte(PUSH1)},
		"push2end": {byte(PUSH1), 1, byte(PUSH2), 0x12},
		"push9end": {byte(PUSH1), 1, byte(PUSH9), 1, 2, 3},
		"badjump":  prog(PUSH1, 0, JUMP),
		"jumpdata": prog(PUSH2, 0x5b, 0x00, PUSH1, 1, JUMP),
		"jumpinot": prog(PUSH1, 0, PUSH1, 0xff, JUMPI, pushes(4)),
		"jumpibad": prog(PUSH1, 1, PUSH1, 0xff, JUMPI),
		"jumpend":  {byte(PUSH1), 3, byte(JUMP), byte(JUMPDEST)},
		// A failed frame returns no data, whatever the last CALL returned.
		"callthenbadjump": {byte(PUSH1), 32, byte(PUSH1), 0, byte(PUSH1), 32, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 4, byte(GAS), byte(CALL), byte(PUSH1), 0, byte(JUMP)},
		"callthenend":     {byte(PUSH1), 32, byte(PUSH1), 0, byte(PUSH1), 32, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 4, byte(GAS), byte(CALL)},
	}
	for op, w := range fastPathOps {
		if w.numPop > 0 {
			programs["under"+op.String()] = prog(pushes(w.numPop-1), op)
		}
		if w.numPush > w.numPop {
			programs["over"+op.String()] = prog(pushes(stackLimit), op)
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	// GAS, MSIZE and the generic stack ops see whether the fast path stored its registers back.
	alphabet := append(slices.Sorted(maps.Keys(fastPathOps)), GAS, MSIZE, NOT, OR)
	for i := range 300 {
		b := pushes(8)
		for range 40 {
			switch rng.IntN(6) {
			case 0, 1:
				b = append(b, byte(PUSH1), byte(rng.IntN(120)))
			case 2:
				b = append(b, byte(PUSH2), byte(rng.IntN(2)), byte(rng.IntN(256)))
			default:
				// Jumps land on their own JUMPDEST, so the program runs on to the tail that returns the stack.
				switch op := alphabet[rng.IntN(len(alphabet))]; op {
				case JUMP:
					dest := len(b) + 4
					b = append(b, byte(PUSH2), byte(dest>>8), byte(dest), byte(JUMP), byte(JUMPDEST))
				case JUMPI:
					dest := len(b) + 6
					b = append(b, byte(PUSH1), byte(rng.IntN(2)), byte(PUSH2), byte(dest>>8), byte(dest), byte(JUMPI), byte(JUMPDEST))
				default:
					b = append(b, byte(op))
					if op.IsPushWithImmediateArgs() {
						for range op - PUSH0 {
							b = append(b, byte(rng.IntN(256)))
						}
					}
				}
			}
		}
		programs[fmt.Sprintf("random%d", i)] = prog(b)
	}
	for name, code := range programs {
		const plenty = 1_000_000
		_, cost := runOnce(code, plenty, false, false)
		budgets := []uint64{plenty, cost / 2, cost - 1, cost, cost + 1}
		if cost < 2000 {
			for gas := range cost + 2 {
				budgets = append(budgets, gas)
			}
		}
		for _, gas := range budgets {
			for _, hooked := range []bool{false, true} {
				want, _ := runOnce(code, gas, false, hooked)
				got, _ := runOnce(code, gas, true, hooked)
				require.Equal(t, want, got, "%s at gas %d hooked %v", name, gas, hooked)
			}
		}
	}
}
