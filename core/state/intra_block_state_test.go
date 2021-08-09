// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/params"
	"gopkg.in/check.v1"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

func TestSnapshotRandom(t *testing.T) {
	config := &quick.Config{MaxCount: 1000}
	err := quick.Check((*snapshotTest).run, config)
	if cerr, ok := err.(*quick.CheckError); ok {
		test := cerr.In[0].(*snapshotTest)
		t.Errorf("%v:\n%s", test.err, test)
	} else if err != nil {
		t.Error(err)
	}
}

// A snapshotTest checks that reverting IntraBlockState snapshots properly undoes all changes
// captured by the snapshot. Instances of this test with pseudorandom content are created
// by Generate.
//
// The test works as follows:
//
// A new state is created and all actions are applied to it. Several snapshots are taken
// in between actions. The test then reverts each snapshot. For each snapshot the actions
// leading up to it are replayed on a fresh, empty state. The behaviour of all public
// accessor methods on the reverted state must match the return value of the equivalent
// methods on the replayed state.
type snapshotTest struct {
	addrs     []common.Address // all account addresses
	actions   []testAction     // modifications to the state
	snapshots []int            // actions indexes at which snapshot is taken
	err       error            // failure details are reported through this field
}

type testAction struct {
	name   string
	fn     func(testAction, *IntraBlockState)
	args   []int64
	noAddr bool
}

// newTestAction creates a random action that changes state.
func newTestAction(addr common.Address, r *rand.Rand) testAction {
	actions := []testAction{
		{
			name: "SetBalance",
			fn: func(a testAction, s *IntraBlockState) {
				s.SetBalance(addr, uint256.NewInt(uint64(a.args[0])))
			},
			args: make([]int64, 1),
		},
		{
			name: "AddBalance",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddBalance(addr, uint256.NewInt(uint64(a.args[0])))
			},
			args: make([]int64, 1),
		},
		{
			name: "SetNonce",
			fn: func(a testAction, s *IntraBlockState) {
				s.SetNonce(addr, uint64(a.args[0]))
			},
			args: make([]int64, 1),
		},
		{
			name: "SetState",
			fn: func(a testAction, s *IntraBlockState) {
				var key common.Hash
				binary.BigEndian.PutUint16(key[:], uint16(a.args[0]))
				val := uint256.NewInt(uint64(a.args[1]))
				s.SetState(addr, &key, *val)
			},
			args: make([]int64, 2),
		},
		{
			name: "SetCode",
			fn: func(a testAction, s *IntraBlockState) {
				code := make([]byte, 16)
				binary.BigEndian.PutUint64(code, uint64(a.args[0]))
				binary.BigEndian.PutUint64(code[8:], uint64(a.args[1]))
				s.SetCode(addr, code)
			},
			args: make([]int64, 2),
		},
		{
			name: "CreateAccount",
			fn: func(a testAction, s *IntraBlockState) {
				s.CreateAccount(addr, true)
			},
		},
		{
			name: "Suicide",
			fn: func(a testAction, s *IntraBlockState) {
				s.Suicide(addr)
			},
		},
		{
			name: "AddRefund",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddRefund(uint64(a.args[0]))
			},
			args:   make([]int64, 1),
			noAddr: true,
		},
		{
			name: "AddLog",
			fn: func(a testAction, s *IntraBlockState) {
				data := make([]byte, 2)
				binary.BigEndian.PutUint16(data, uint16(a.args[0]))
				s.AddLog(&types.Log{Address: addr, Data: data})
			},
			args: make([]int64, 1),
		},
		{
			name: "AddAddressToAccessList",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddAddressToAccessList(addr)
			},
		},
		{
			name: "AddSlotToAccessList",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddSlotToAccessList(addr,
					common.Hash{byte(a.args[0])})
			},
			args: make([]int64, 1),
		},
	}
	action := actions[r.Intn(len(actions))]
	var nameargs []string
	if !action.noAddr {
		nameargs = append(nameargs, addr.Hex())
	}
	for i := range action.args {
		action.args[i] = rand.Int63n(100)
		nameargs = append(nameargs, fmt.Sprint(action.args[i]))
	}
	action.name += strings.Join(nameargs, ", ")
	return action
}

// Generate returns a new snapshot test of the given size. All randomness is
// derived from r.
func (*snapshotTest) Generate(r *rand.Rand, size int) reflect.Value {
	// Generate random actions.
	addrs := make([]common.Address, 50)
	for i := range addrs {
		addrs[i][0] = byte(i)
	}
	actions := make([]testAction, size)
	for i := range actions {
		addr := addrs[r.Intn(len(addrs))]
		actions[i] = newTestAction(addr, r)
	}
	// Generate snapshot indexes.
	nsnapshots := int(math.Sqrt(float64(size)))
	if size > 0 && nsnapshots == 0 {
		nsnapshots = 1
	}
	snapshots := make([]int, nsnapshots)
	snaplen := len(actions) / nsnapshots
	for i := range snapshots {
		// Try to place the snapshots some number of actions apart from each other.
		snapshots[i] = (i * snaplen) + r.Intn(snaplen)
	}
	return reflect.ValueOf(&snapshotTest{addrs, actions, snapshots, nil})
}

func (test *snapshotTest) String() string {
	out := new(bytes.Buffer)
	sindex := 0
	for i, action := range test.actions {
		if len(test.snapshots) > sindex && i == test.snapshots[sindex] {
			fmt.Fprintf(out, "---- snapshot %d ----\n", sindex)
			sindex++
		}
		fmt.Fprintf(out, "%4d: %s\n", i, action.name)
	}
	return out.String()
}

func (test *snapshotTest) run() bool {
	// Run all actions and create snapshots.
	db := memdb.New()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		test.err = err
		return false
	}
	defer tx.Rollback()
	var (
		ds           = NewPlainState(tx, 0)
		state        = New(ds)
		snapshotRevs = make([]int, len(test.snapshots))
		sindex       = 0
	)
	for i, action := range test.actions {
		if len(test.snapshots) > sindex && i == test.snapshots[sindex] {
			snapshotRevs[sindex] = state.Snapshot()
			sindex++
		}
		action.fn(action, state)
	}
	// Revert all snapshots in reverse order. Each revert must yield a state
	// that is equivalent to fresh state with all actions up the snapshot applied.
	for sindex--; sindex >= 0; sindex-- {
		checkds := NewPlainState(tx, 0)
		checkstate := New(checkds)
		for _, action := range test.actions[:test.snapshots[sindex]] {
			action.fn(action, checkstate)
		}
		state.RevertToSnapshot(snapshotRevs[sindex])
		if err := test.checkEqual(state, checkstate); err != nil {
			test.err = fmt.Errorf("state mismatch after revert to snapshot %d\n%v", sindex, err)
			return false
		}
	}
	return true
}

// checkEqual checks that methods of state and checkstate return the same values.
func (test *snapshotTest) checkEqual(state, checkstate *IntraBlockState) error {
	for _, addr := range test.addrs {
		addr := addr // pin
		var err error
		checkeq := func(op string, a, b interface{}) bool {
			if err == nil && !reflect.DeepEqual(a, b) {
				err = fmt.Errorf("got %s(%s) == %v, want %v", op, addr.Hex(), a, b)
				return false
			}
			return true
		}
		checkeqBigInt := func(op string, a, b *big.Int) bool {
			if err == nil && a.Cmp(b) != 0 {
				err = fmt.Errorf("got %s(%s) == %d, want %d", op, addr.Hex(), a, b)
				return false
			}
			return true
		}
		// Check basic accessor methods.
		if !checkeq("Exist", state.Exist(addr), checkstate.Exist(addr)) {
			return err
		}
		checkeq("HasSuicided", state.HasSuicided(addr), checkstate.HasSuicided(addr))
		checkeqBigInt("GetBalance", state.GetBalance(addr).ToBig(), checkstate.GetBalance(addr).ToBig())
		checkeq("GetNonce", state.GetNonce(addr), checkstate.GetNonce(addr))
		checkeq("GetCode", state.GetCode(addr), checkstate.GetCode(addr))
		checkeq("GetCodeHash", state.GetCodeHash(addr), checkstate.GetCodeHash(addr))
		checkeq("GetCodeSize", state.GetCodeSize(addr), checkstate.GetCodeSize(addr))
		// Check storage.
		if obj := state.getStateObject(addr); obj != nil {
			for key, value := range obj.dirtyStorage {
				var out uint256.Int
				checkstate.GetState(addr, &key, &out)
				if !checkeq("GetState("+key.Hex()+")", out, value) {
					return err
				}
			}
		}
		if obj := checkstate.getStateObject(addr); obj != nil {
			for key, value := range obj.dirtyStorage {
				var out uint256.Int
				state.GetState(addr, &key, &out)
				if !checkeq("GetState("+key.Hex()+")", out, value) {
					return err
				}
			}
		}
	}

	if state.GetRefund() != checkstate.GetRefund() {
		return fmt.Errorf("got GetRefund() == %d, want GetRefund() == %d",
			state.GetRefund(), checkstate.GetRefund())
	}
	if !reflect.DeepEqual(state.GetLogs(common.Hash{}), checkstate.GetLogs(common.Hash{})) {
		return fmt.Errorf("got GetLogs(common.Hash{}) == %v, want GetLogs(common.Hash{}) == %v",
			state.GetLogs(common.Hash{}), checkstate.GetLogs(common.Hash{}))
	}
	return nil
}

func (s *StateSuite) TestTouchDelete(c *check.C) {
	s.state.GetOrNewStateObject(common.Address{})

	err := s.state.FinalizeTx(params.Rules{}, s.w)
	if err != nil {
		c.Fatal("error while finalize", err)
	}

	err = s.state.CommitBlock(params.Rules{}, s.w)
	if err != nil {
		c.Fatal("error while commit", err)
	}

	s.state.Reset()

	snapshot := s.state.Snapshot()
	s.state.AddBalance(common.Address{}, new(uint256.Int))

	if len(s.state.journal.dirties) != 1 {
		c.Fatal("expected one dirty state object")
	}
	s.state.RevertToSnapshot(snapshot)
	if len(s.state.journal.dirties) != 0 {
		c.Fatal("expected no dirty state object")
	}
}

func TestAccessList(t *testing.T) {
	// Some helpers
	addr := common.HexToAddress
	slot := common.HexToHash

	_, tx := memdb.NewTestTx(t)
	state := New(NewPlainState(tx, 0))
	state.accessList = newAccessList()

	verifyAddrs := func(astrings ...string) {
		t.Helper()
		// convert to common.Address form
		var addresses []common.Address
		var addressMap = make(map[common.Address]struct{})
		for _, astring := range astrings {
			address := addr(astring)
			addresses = append(addresses, address)
			addressMap[address] = struct{}{}
		}
		// Check that the given addresses are in the access list
		for _, address := range addresses {
			if !state.AddressInAccessList(address) {
				t.Fatalf("expected %x to be in access list", address)
			}
		}
		// Check that only the expected addresses are present in the acesslist
		for address := range state.accessList.addresses {
			if _, exist := addressMap[address]; !exist {
				t.Fatalf("extra address %x in access list", address)
			}
		}
	}
	verifySlots := func(addrString string, slotStrings ...string) {
		if !state.AddressInAccessList(addr(addrString)) {
			t.Fatalf("scope missing address/slots %v", addrString)
		}
		var address = addr(addrString)
		// convert to common.Hash form
		var slots []common.Hash
		var slotMap = make(map[common.Hash]struct{})
		for _, slotString := range slotStrings {
			s := slot(slotString)
			slots = append(slots, s)
			slotMap[s] = struct{}{}
		}
		// Check that the expected items are in the access list
		for i, s := range slots {
			if _, slotPresent := state.SlotInAccessList(address, s); !slotPresent {
				t.Fatalf("input %d: scope missing slot %v (address %v)", i, s, addrString)
			}
		}
		// Check that no extra elements are in the access list
		index := state.accessList.addresses[address]
		if index >= 0 {
			stateSlots := state.accessList.slots[index]
			for s := range stateSlots {
				if _, slotPresent := slotMap[s]; !slotPresent {
					t.Fatalf("scope has extra slot %v (address %v)", s, addrString)
				}
			}
		}
	}

	state.AddAddressToAccessList(addr("aa"))          // 1
	state.AddSlotToAccessList(addr("bb"), slot("01")) // 2,3
	state.AddSlotToAccessList(addr("bb"), slot("02")) // 4
	verifyAddrs("aa", "bb")
	verifySlots("bb", "01", "02")

	// Make a copy
	stateCopy1 := state.Copy()
	if exp, got := 4, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}

	// same again, should cause no journal entries
	state.AddSlotToAccessList(addr("bb"), slot("01"))
	state.AddSlotToAccessList(addr("bb"), slot("02"))
	state.AddAddressToAccessList(addr("aa"))
	if exp, got := 4, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}
	// some new ones
	state.AddSlotToAccessList(addr("bb"), slot("03")) // 5
	state.AddSlotToAccessList(addr("aa"), slot("01")) // 6
	state.AddSlotToAccessList(addr("cc"), slot("01")) // 7,8
	state.AddAddressToAccessList(addr("cc"))
	if exp, got := 8, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}

	verifyAddrs("aa", "bb", "cc")
	verifySlots("aa", "01")
	verifySlots("bb", "01", "02", "03")
	verifySlots("cc", "01")

	// now start rolling back changes
	state.journal.revert(state, 7)
	if _, ok := state.SlotInAccessList(addr("cc"), slot("01")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs("aa", "bb", "cc")
	verifySlots("aa", "01")
	verifySlots("bb", "01", "02", "03")

	state.journal.revert(state, 6)
	if state.AddressInAccessList(addr("cc")) {
		t.Fatalf("addr present, expected missing")
	}
	verifyAddrs("aa", "bb")
	verifySlots("aa", "01")
	verifySlots("bb", "01", "02", "03")

	state.journal.revert(state, 5)
	if _, ok := state.SlotInAccessList(addr("aa"), slot("01")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs("aa", "bb")
	verifySlots("bb", "01", "02", "03")

	state.journal.revert(state, 4)
	if _, ok := state.SlotInAccessList(addr("bb"), slot("03")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs("aa", "bb")
	verifySlots("bb", "01", "02")

	state.journal.revert(state, 3)
	if _, ok := state.SlotInAccessList(addr("bb"), slot("02")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs("aa", "bb")
	verifySlots("bb", "01")

	state.journal.revert(state, 2)
	if _, ok := state.SlotInAccessList(addr("bb"), slot("01")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs("aa", "bb")

	state.journal.revert(state, 1)
	if state.AddressInAccessList(addr("bb")) {
		t.Fatalf("addr present, expected missing")
	}
	verifyAddrs("aa")

	state.journal.revert(state, 0)
	if state.AddressInAccessList(addr("aa")) {
		t.Fatalf("addr present, expected missing")
	}
	if got, exp := len(state.accessList.addresses), 0; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}
	if got, exp := len(state.accessList.slots), 0; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}
	// Check the copy
	// Make a copy
	state = stateCopy1
	verifyAddrs("aa", "bb")
	verifySlots("bb", "01", "02")
	if got, exp := len(state.accessList.addresses), 2; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}
	if got, exp := len(state.accessList.slots), 1; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}
}
