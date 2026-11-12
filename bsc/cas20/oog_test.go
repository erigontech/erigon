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

package cas20

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// chargeGas only marks the frame; the state is discarded either way, but the node
// must not do the work first.
func TestCAS20ExhaustedBudgetStopsWork(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x0c9"), creator,
			[][]byte{cas20Call(selGrantRole, roleMint, addrKey(creator))}),
		NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	const n = 4000
	recips := make([]common.Address, n)
	amts := make([]uint64, n)
	for i := range recips {
		recips[i] = common.BigToAddress(uint256.NewInt(uint64(0x400000 + i)).ToBig())
		amts[i] = 1
	}
	input := encodeBatchMint(recips, amts)

	best := func(budget uint64, wantErr bool) time.Duration {
		t.Helper()
		out := time.Hour
		for range 5 {
			var err error
			statedb.run(func() {
				e := newTestEVM(cas20BlockContext(1), statedb, cas20TestChainConfig())
				start := time.Now()
				_, _, err = e.Call(creator, token, input, NewGasBudget(budget), uint256.NewInt(0))
				if d := time.Since(start); d < out {
					out = d
				}
			})
			if wantErr && err == nil {
				t.Fatalf("a %d-recipient batch on %d gas should not succeed", n, budget)
			}
			if !wantErr && err != nil {
				t.Fatalf("a %d-recipient batch on %d gas should succeed: %v", n, budget, err)
			}
		}
		return out
	}

	// 25,000 gas cannot pay the calldata charge, so the batch must abandon at once.
	starved, funded := best(25_000, true), best(200_000_000, false)
	if starved*3 > funded {
		t.Errorf("a starved batch took %v against %v for one that could pay — the loop "+
			"is still running past exhaustion", starved, funded)
	}
}

// The same property for the initCalls loop, the most expensive per iteration.
func TestCAS20BootstrapStopsOnExhaustion(t *testing.T) {
	creator := common.HexToAddress("0xc4ea70")

	const n = 1500
	calls := make([][]byte, 0, n+1)
	for i := range n {
		who := common.BigToAddress(uint256.NewInt(uint64(0x500000 + i)).ToBig())
		calls = append(calls, cas20Call(selGrantRole, roleMint, addrKey(who)))
	}
	input := encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xb007"), creator, calls)

	best := func(budget uint64, wantErr bool) time.Duration {
		t.Helper()
		out := time.Hour
		for range 5 {
			_, e := newCAS20EVM(t)
			start := time.Now()
			_, _, err := e.Call(creator, CAS20FactoryAddress, input, NewGasBudget(budget), uint256.NewInt(0))
			if d := time.Since(start); d < out {
				out = d
			}
			if wantErr && err == nil {
				t.Fatalf("a %d-call bootstrap on %d gas should not succeed", n, budget)
			}
			if !wantErr && err != nil {
				t.Fatalf("a %d-call bootstrap on %d gas should succeed: %v", n, budget, err)
			}
		}
		return out
	}

	starved, funded := best(60_000, true), best(400_000_000, false)
	if starved*3 > funded {
		t.Errorf("a starved bootstrap took %v against %v for one that could pay — the "+
			"initCalls loop is still running past exhaustion", starved, funded)
	}
}

// The one loop whose bound comes from state, not calldata: the old tail's length
// is whatever a previous caller paid to store.
func TestCAS20OldTailReleaseStopsOnExhaustion(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x0"), creator,
			[][]byte{cas20Call(selGrantRole, roleMetadata, addrKey(creator))}),
		NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	long := strings.Repeat("N", 40_000)
	if _, _, err := evm.Call(creator, token, encodeStringCall(selUpdateName, long),
		NewGasBudget(500_000_000), uint256.NewInt(0)); err != nil {
		t.Fatalf("storing the long name: %v", err)
	}
	chunks := newUnmeteredCAS20Storage(statedb, token).stringChunks(slotAt(cas20SlotName))
	if chunks < 1000 {
		t.Fatalf("the long name occupies %d chunks, too few for this to measure", chunks)
	}

	short := encodeStringCall(selUpdateName, "x")
	best := func(budget uint64, wantErr bool) time.Duration {
		t.Helper()
		out := time.Hour
		for range 5 {
			var err error
			statedb.run(func() {
				e := newTestEVM(cas20BlockContext(1), statedb, cas20TestChainConfig())
				start := time.Now()
				_, _, err = e.Call(creator, token, short, NewGasBudget(budget), uint256.NewInt(0))
				if d := time.Since(start); d < out {
					out = d
				}
				// The surviving long name is what makes the work re-buyable.
				if wantErr {
					if got := newUnmeteredCAS20Storage(statedb, token).stringChunks(slotAt(cas20SlotName)); got != chunks {
						t.Fatalf("after a reverted attempt the name occupies %d chunks, want %d", got, chunks)
					}
				}
			})
			if wantErr && err == nil {
				t.Fatalf("a starved updateName should not succeed")
			}
			if !wantErr && err != nil {
				t.Fatalf("a funded updateName should succeed: %v", err)
			}
		}
		return out
	}

	starved, funded := best(30_000, true), best(200_000_000, false)
	if starved*3 > funded {
		t.Errorf("a starved release took %v against %v for one that could pay — the "+
			"old-tail loop is still running past exhaustion", starved, funded)
	}
}

func warmed(db *testState, addr common.Address, slot common.Hash) bool {
	_, ok := db.SlotInAccessList(addr, slot)
	return ok
}

func TestCAS20UnaffordableCallDoesNoWork(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xdec0de")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x006"), creator, [][]byte{
			cas20Call(selGrantRole, roleMint, addrKey(creator)),
			cas20Call(selMint, addrKey(cas20Alice), u256hash(100)),
		}), NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	slot := cas20Storage{token: token}.balanceSlot(cas20Carol)
	if warmed(statedb, token, slot) {
		t.Fatal("carol's slot is already warm; the fixture cannot show a skipped read")
	}

	// Without evm.Call's snapshot, which would revert the access list and read the
	// slot cold whether or not the handler touched it.
	p, ok := resolveCAS20(token)
	if !ok {
		t.Fatal("the token address does not resolve to a precompile")
	}
	input := cas20Call(selBalanceOf, addrKey(cas20Carol))
	out, left, err := runStatefulPrecompiledContract(evm, p,
		creator, token, input, NewGasBudget(1), false, true, uint256.NewInt(0))
	if !errors.Is(err, vm.ErrOutOfGas) {
		t.Errorf("an unaffordable call err = %v, want vm.ErrOutOfGas", err)
	}
	if len(out) != 0 {
		t.Errorf("returndata = %x, want empty", out)
	}
	if left.Execution != 0 {
		t.Errorf("gas left = %d, want 0", left.Execution)
	}
	if warmed(statedb, token, slot) {
		t.Error("the handler read state it could not pay for: carol's slot is warm after a " +
			"call that could not afford its calldata. RequiredGas is zero, so the entry check " +
			"is the only thing standing between a ~100-gas CALL and a handler's worth of work")
	}
}

func TestCAS20CalldataGasMatchesTheBEP(t *testing.T) {
	charged := func(words int) uint64 {
		statedb := newTestStateDB(t)
		gas := NewGasBudget(10_000_000)
		ctx := &PrecompileContext{StateDB: statedb, Self: cas20Addr(cas20VariantAsset, 1), gas: &gas}
		before := gas.Execution
		ctx.chargeCalldata(make([]byte, words*32))
		return before - gas.Execution
	}
	for _, words := range []int{0, 1, 2, 10, 1000} {
		want := uint64(words) * (params.CopyGas + params.MemoryGas)
		if got := charged(words); got != want {
			t.Errorf("%d words charged %d, want %d — the table in BEP-702 3.14 is exhaustive "+
				"and forbids synthesizing opcode or memory-expansion overhead", words, got, want)
		}
	}
	// A quadratic term would show up in the differences even if the absolute values matched.
	if d := charged(1000) - charged(999); d != params.CopyGas+params.MemoryGas {
		t.Errorf("the 1000th word costs %d, want %d — the charge is not linear",
			d, params.CopyGas+params.MemoryGas)
	}
}

// A read zeroed for want of gas once let announce publish a complete disclosure on
// a refused frame. EndAnnouncement is the observable: it is the last write.
func TestCAS20AnnounceStopsAtTheUnpaidRead(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	admin := cas20TestCaller
	ret, _, err := evm.Call(admin, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xaa11"), admin, [][]byte{
			cas20Call(selGrantRole, roleOperator, addrKey(admin)),
		}), NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	p, ok := resolveCAS20(token)
	if !ok {
		t.Fatal("the token does not resolve")
	}

	// An empty calls array so the loop guard never gets a turn; a fresh id per probe
	// so each meets a cold slot.
	long := strings.Repeat("x", 600)

	probes, refused, zero := 0, 0, 0
	for g := uint64(2_000); g <= 320_000; g += 2_000 {
		before := len(statedb.Logs())
		_, _, err := runStatefulPrecompiledContract(evm, p,
			admin, token, encodeAnnounceWith(nil, fmt.Sprintf("announcement-%d", g), long, long),
			NewGasBudget(g), false, true, uint256.NewInt(0))
		added := statedb.Logs()[before:]
		probes++
		if err == nil {
			continue
		}
		refused++
		if !errors.Is(err, vm.ErrOutOfGas) {
			t.Fatalf("budget %d: err = %v, want vm.ErrOutOfGas", g, err)
		}
		if len(added) == 0 {
			zero++
		}
		// A log the frame paid for may be here; EndAnnouncement, emitted after the
		// refusal point, may not.
		for _, l := range added {
			if l.Topics[0] == cas20TopicEndAnnouncement {
				t.Fatalf("budget %d: the call ran out of gas yet reached EndAnnouncement, "+
					"so it kept executing after a charge was refused", g)
			}
		}
	}
	if refused == 0 || refused == probes {
		t.Fatalf("%d of %d probes were refused; the sweep needs both outcomes to be "+
			"straddling the boundary", refused, probes)
	}
	if zero == 0 {
		t.Error("no probe was refused before its first log, so the sweep never reached " +
			"the unpaid read this test is named for")
	}
}

// Two independent barriers stand between an unpaid read and a wrong answer, so no
// single-mutation test can witness this; the end-to-end property is what is pinned.
func TestCAS20AnnouncementViewNeverAnswersFromAnUnpaidRead(t *testing.T) {
	_, evm := newCAS20EVM(t)
	admin := cas20TestCaller
	ret, _, err := evm.Call(admin, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xbb22"), admin, [][]byte{
			cas20Call(selGrantRole, roleOperator, addrKey(admin)),
		}), NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	p, ok := resolveCAS20(token)
	if !ok {
		t.Fatal("the token does not resolve")
	}

	const id = "disclosure-1"
	if _, _, err := evm.Call(admin, token, encodeAnnounceWith(nil, id, "d", "u"),
		NewGasBudget(1_000_000), uint256.NewInt(0)); err != nil {
		t.Fatalf("announce: %v", err)
	}

	input := encodeStringCall(selIsAnnouncementIdUsed, id)
	answered, refused := 0, 0
	for g := uint64(20); g <= 2_000; g += 20 {
		out, _, err := runStatefulPrecompiledContract(evm, p,
			admin, token, input, NewGasBudget(g), true, true, uint256.NewInt(0))
		if err != nil {
			if !errors.Is(err, vm.ErrOutOfGas) {
				t.Fatalf("budget %d: err = %v, want vm.ErrOutOfGas", g, err)
			}
			refused++
			continue
		}
		answered++
		if !bytes.Equal(out, encBool(true)) {
			t.Fatalf("budget %d: the view answered %x for an id that was announced. A read "+
				"that could not be paid for must fail the call, never default to zero", g, out)
		}
	}
	if answered == 0 || refused == 0 {
		t.Fatalf("%d answered, %d refused; the sweep needs both to straddle the read's price",
			answered, refused)
	}
}

// One entry of an announce bundle or an initCalls array costs what the reference
// contract pays to route it: a warm CALL plus the copy of the entry. Aliased
// offsets are accepted, so without this a shared tail could be dispatched N×M
// times for the price of one.
func TestCAS20InternalDispatchIsCharged(t *testing.T) {
	_, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xdec0de")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xd15"), creator, [][]byte{
			cas20Call(selGrantRole, roleOperator, addrKey(creator)),
		}), NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	// A constant getter reads no state, so the difference between announcing n
	// and n+1 of them is the dispatch charge alone, once the id's own cost is out.
	used := func(n int, id string) uint64 {
		calls := make([][]byte, n)
		for i := range calls {
			calls[i] = cas20Call(selWadPrecision)
		}
		gas := NewGasBudget(5_000_000)
		_, left, err := evm.Call(creator, token, encodeAnnounce(calls, id), gas, uint256.NewInt(0))
		if err != nil {
			t.Fatalf("announce with %d calls: %v", n, err)
		}
		return gas.Execution - left.Execution
	}
	one, two := used(1, "id-1"), used(2, "id-2")
	// The second announce also carries three more words of outer calldata: the
	// entry's offset, its length and its data.
	want := params.WarmStorageReadCostEIP2929 + cas20CalldataWordGas + 3*cas20CalldataWordGas
	if two-one != want {
		t.Errorf("one more constant getter cost %d, want %d (warm CALL + its input word + three calldata words)", two-one, want)
	}

	// The charge is taken before each entry runs, so the budget decides how far
	// into the bundle execution gets. Budget for the announcement itself plus
	// exactly 200 dispatches succeeds; the same budget short of the last 100
	// dispatches is out of gas — and only the dispatch charges separate the two.
	many := make([][]byte, 200)
	for i := range many {
		many[i] = cas20Call(selWadPrecision)
	}
	perEntry := params.WarmStorageReadCostEIP2929 + cas20CalldataWordGas + 3*cas20CalldataWordGas
	enough := one + 199*perEntry
	if _, _, err := evm.Call(creator, token, encodeAnnounce(many, "id-200"), NewGasBudget(enough), uint256.NewInt(0)); err != nil {
		t.Fatalf("200 dispatches on their exact price: %v", err)
	}
	if _, _, err := evm.Call(creator, token, encodeAnnounce(many, "id-200-short"), NewGasBudget(enough-100*perEntry), uint256.NewInt(0)); !errors.Is(err, vm.ErrOutOfGas) {
		t.Errorf("200 dispatches short of the last hundred: err = %v, want out of gas", err)
	}
}
