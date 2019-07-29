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
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"gopkg.in/check.v1"

	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// Tests that updating a state trie does not leak any database writes prior to
// actually committing the state.
func TestUpdateLeaks(t *testing.T) {
	// Create an empty state database
	db := ethdb.NewMemDatabase()
	tds, _ := NewTrieDbState(context.Background(), common.Hash{}, db, 0)
	state := New(tds)

	// Update it with some accounts
	for i := byte(0); i < 255; i++ {
		tds.StartNewBuffer()
		addr := common.BytesToAddress([]byte{i})
		state.AddBalance(addr, big.NewInt(int64(11*i)))
		state.SetNonce(addr, uint64(42*i))
		if i%2 == 0 {
			state.SetState(addr, common.BytesToHash([]byte{i, i, i}), common.BytesToHash([]byte{i, i, i, i}))
		}
		if i%3 == 0 {
			state.SetCode(addr, []byte{i, i, i, i, i})
		}
		_ = state.FinalizeTx(context.Background(), tds.TrieStateWriter())
	}

	_, err := tds.ComputeTrieRoots(context.Background())
	if err != nil {
		t.Fatal("error while ComputeTrieRoots", err)
	}

	// Ensure that no data was leaked into the database
	for keys, i := db.Keys(), 0; i < len(keys); i += 2 {
		if bytes.Equal(keys[i], trie.SecureKeyPrefix) {
			continue
		}
		value, _ := db.Get(keys[i], keys[i+1])
		t.Errorf("State leaked into database: %x:%x -> %x", keys[i], keys[i+1], value)
	}
}

// Tests that no intermediate state of an object is stored into the database,
// only the one right before the commit.
func TestIntermediateLeaks(t *testing.T) {
	// Create two state databases, one transitioning to the final state, the other final from the beginning
	transDb := ethdb.NewMemDatabase()
	finalDb := ethdb.NewMemDatabase()
	transTds, _ := NewTrieDbState(context.Background(), common.Hash{}, transDb, 0)
	transState := New(transTds)
	transTds.StartNewBuffer()
	finalTds, _ := NewTrieDbState(context.Background(), common.Hash{}, finalDb, 0)
	finalState := New(finalTds)
	finalTds.StartNewBuffer()

	modify := func(state *IntraBlockState, addr common.Address, i, tweak byte) {
		state.SetBalance(addr, big.NewInt(int64(11*i)+int64(tweak)))
		state.SetNonce(addr, uint64(42*i+tweak))
		if i%2 == 0 {
			state.SetState(addr, common.Hash{i, i, i, 0}, common.Hash{})
			state.SetState(addr, common.Hash{i, i, i, tweak}, common.Hash{i, i, i, i, tweak})
		}
		if i%3 == 0 {
			state.SetCode(addr, []byte{i, i, i, i, i, tweak})
		}
	}

	// Modify the transient state.
	for i := byte(0); i < 255; i++ {
		modify(transState, common.Address{byte(i)}, i, 0)
	}

	// Write modifications to trie.
	err := transState.FinalizeTx(context.Background(), transTds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalizing state", err)
	}

	transTds.StartNewBuffer()

	// Overwrite all the data with new values in the transient database.
	for i := byte(0); i < 255; i++ {
		modify(transState, common.Address{byte(i)}, i, 99)
		modify(finalState, common.Address{byte(i)}, i, 99)
	}

	// Commit and cross check the databases.
	err = transState.FinalizeTx(context.Background(), transTds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalizing state", err)
	}

	_, err = transTds.ComputeTrieRoots(context.Background())
	if err != nil {
		t.Fatal("error while ComputeTrieRoots", err)
	}

	transTds.SetBlockNr(context.Background(), 1)

	err = transState.CommitBlock(context.Background(), transTds.DbStateWriter())
	if err != nil {
		t.Fatal("failed to commit transition state", err)
	}

	err = finalState.FinalizeTx(context.Background(), finalTds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalizing state", err)
	}

	_, err = finalTds.ComputeTrieRoots(context.Background())
	if err != nil {
		t.Fatal("error while ComputeTrieRoots", err)
	}

	finalTds.SetBlockNr(context.Background(), 1)
	if err := finalState.CommitBlock(context.Background(), finalTds.DbStateWriter()); err != nil {
		t.Fatalf("failed to commit final state: %v", err)
	}
	for finalKeys, i := finalDb.Keys(), 0; i < len(finalKeys); i += 2 {
		if bytes.Equal(finalKeys[i], trie.SecureKeyPrefix) {
			continue
		}
		if _, err := transDb.Get(finalKeys[i], finalKeys[i+1]); err != nil {
			val, _ := finalDb.Get(finalKeys[i], finalKeys[i+1])
			t.Errorf("entry missing from the transition database: %x:%x -> %x", finalKeys[i], finalKeys[i+1], val)
		}
	}
	for transKeys, i := transDb.Keys(), 0; i < len(transKeys); i += 2 {
		if bytes.Equal(transKeys[i], trie.SecureKeyPrefix) {
			continue
		}
		if _, err := finalDb.Get(transKeys[i], transKeys[i+1]); err != nil {
			val, _ := transDb.Get(transKeys[i], transKeys[i+1])
			t.Errorf("entry missing in the transition database: %x:%x -> %x", transKeys[i], transKeys[i+1], val)
		}
	}
}

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
				s.SetBalance(addr, big.NewInt(a.args[0]))
			},
			args: make([]int64, 1),
		},
		{
			name: "AddBalance",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddBalance(addr, big.NewInt(a.args[0]))
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
				var key, val common.Hash
				binary.BigEndian.PutUint16(key[:], uint16(a.args[0]))
				binary.BigEndian.PutUint16(val[:], uint16(a.args[1]))
				s.SetState(addr, key, val)
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
			name: "AddPreimage",
			fn: func(a testAction, s *IntraBlockState) {
				preimage := []byte{1}
				hash := common.BytesToHash(preimage)
				s.AddPreimage(hash, preimage)
			},
			args: make([]int64, 1),
		},
	}
	action := actions[r.Intn(len(actions))]
	var nameargs []string
	if !action.noAddr {
		nameargs = append(nameargs, addr.Hex())
	}
	for _, i := range action.args {
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
	var (
		db           = ethdb.NewMemDatabase()
		ds           = NewDbState(db, 0)
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
		checkds := NewDbState(db, 0)
		checkstate := New(checkds)
		for _, action := range test.actions[:test.snapshots[sindex]] {
			action.fn(action, checkstate)
		}
		state.RevertToSnapshot(snapshotRevs[sindex])
		if err := test.checkEqual(state, checkstate, ds, checkds); err != nil {
			test.err = fmt.Errorf("state mismatch after revert to snapshot %d\n%v", sindex, err)
			return false
		}
	}
	return true
}

// checkEqual checks that methods of state and checkstate return the same values.
func (test *snapshotTest) checkEqual(state, checkstate *IntraBlockState, ds, checkds *DbState) error {
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
		// Check basic accessor methods.
		checkeq("Exist", state.Exist(addr), checkstate.Exist(addr))
		checkeq("HasSuicided", state.HasSuicided(addr), checkstate.HasSuicided(addr))
		checkeq("GetBalance", state.GetBalance(addr), checkstate.GetBalance(addr))
		checkeq("GetNonce", state.GetNonce(addr), checkstate.GetNonce(addr))
		checkeq("GetCode", state.GetCode(addr), checkstate.GetCode(addr))
		checkeq("GetCodeHash", state.GetCodeHash(addr), checkstate.GetCodeHash(addr))
		checkeq("GetCodeSize", state.GetCodeSize(addr), checkstate.GetCodeSize(addr))
		// Check storage.
		if obj := state.GetStateObject(addr); obj != nil {
			ds.ForEachStorage(addr, []byte{} /*startKey*/, func(key, seckey, value common.Hash) bool {
				return checkeq("GetState("+key.Hex()+")", checkstate.GetState(addr, key), value)
			}, 1000)
			checkds.ForEachStorage(addr, []byte{} /*startKey*/, func(key, seckey, value common.Hash) bool {
				return checkeq("GetState("+key.Hex()+")", state.GetState(addr, key), value)
			}, 1000)
		}
		if err != nil {
			return err
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

	err := s.state.FinalizeTx(context.Background(), s.tds.TrieStateWriter())
	if err != nil {
		c.Fatal("error while finalize", err)
	}

	_, err = s.tds.ComputeTrieRoots(context.Background())
	if err != nil {
		c.Fatal("error while ComputeTrieRoots", err)
	}

	s.tds.SetBlockNr(context.Background(), 1)

	err = s.state.CommitBlock(context.Background(), s.tds.DbStateWriter())
	if err != nil {
		c.Fatal("error while commit", err)
	}

	s.state.Reset()

	snapshot := s.state.Snapshot()
	s.state.AddBalance(common.Address{}, new(big.Int))

	if len(s.state.journal.dirties) != 1 {
		c.Fatal("expected one dirty state object")
	}
	s.state.RevertToSnapshot(snapshot)
	if len(s.state.journal.dirties) != 0 {
		c.Fatal("expected no dirty state object")
	}
}

// TestCopyOfCopy tests that modified objects are carried over to the copy, and the copy of the copy.
// See https://github.com/ledgerwatch/turbo-geth/pull/15225#issuecomment-380191512
func TestCopyOfCopy(t *testing.T) {
	db := ethdb.NewMemDatabase()
	sdbTds, _ := NewTrieDbState(context.Background(), common.Hash{}, db, 0)
	sdb := New(sdbTds)
	sdbTds.StartNewBuffer()
	addr := common.HexToAddress("aaaa")
	sdb.SetBalance(addr, big.NewInt(42))

	if got := sdb.Copy().GetBalance(addr).Uint64(); got != 42 {
		t.Fatalf("1st copy fail, expected 42, got %v", got)
	}
	if got := sdb.Copy().Copy().GetBalance(addr).Uint64(); got != 42 {
		t.Fatalf("2nd copy fail, expected 42, got %v", got)
	}
}

func TestIntraBlockStateNewEmptyAccount(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tds, _ := NewTrieDbState(context.Background(), common.Hash{}, db, 0)
	state := New(tds)
	addr := common.Address{1}
	state.CreateAccount(addr, true)
	obj := state.GetStateObject(addr)
	if obj.data.StorageSize != nil {
		t.Fatal("Storage size of empty account should be 0", obj.data.StorageSize)
	}
}

func TestIntraBlockStateNewContractAccount(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tds, _ := NewTrieDbState(context.Background(), common.Hash{}, db, 0)
	state := New(tds)
	addr := common.Address{2}
	newObj, _ := state.createObject(addr, nil)
	newObj.code = []byte("some non empty byte code")
	state.setStateObject(newObj)
	state.CreateAccount(common.Address{2}, true)
	obj := state.GetStateObject(addr)
	if obj.data.StorageSize != nil {
		t.Fatal("Storage size of empty account should be nil", obj.data.StorageSize)
	}

	state.IncreaseStorageSize(addr)
	obj = state.GetStateObject(addr)
	if *obj.data.StorageSize != HugeNumber+1 {
		t.Fatal("Storage size of empty account should be HugeNumber +1", *obj.data.StorageSize, HugeNumber)
	}

	state.DecreaseStorageSize(addr)
	state.DecreaseStorageSize(addr)
	obj = state.GetStateObject(addr)
	if *obj.data.StorageSize != HugeNumber-1 {
		t.Fatal("Storage size of empty account should be HugeNumber - 1", *obj.data.StorageSize, HugeNumber, *obj.data.StorageSize-HugeNumber)
	}

}

// TestCopy tests that copying a statedb object indeed makes the original and
// the copy independent of each other. This test is a regression test against
// https://github.com/ethereum/go-ethereum/pull/15549.
func TestCopy(t *testing.T) {
	// Create a random state test to copy and modify "independently"
	db := ethdb.NewMemDatabase()
	origTds, err := NewTrieDbState(context.Background(), common.Hash{}, db, 0)
	if err != nil {
		t.Log(err)
	}

	orig := New(origTds)
	origTds.StartNewBuffer()

	for i := byte(0); i < 255; i++ {
		obj := orig.GetOrNewStateObject(common.BytesToAddress([]byte{i}))
		obj.AddBalance(big.NewInt(int64(i)))
		err = origTds.TrieStateWriter().UpdateAccountData(context.Background(), obj.address, &obj.data, new(accounts.Account))
		if err != nil {
			t.Log(err)
		}
	}

	err = orig.FinalizeTx(context.Background(), origTds.TrieStateWriter())
	if err != nil {
		t.Log("error while finalize", err)
	}

	_, err = origTds.ComputeTrieRoots(context.Background())
	if err != nil {
		t.Log("error while ComputeTrieRoots", err)
	}

	origTds.SetBlockNr(context.Background(), 1)

	err = orig.CommitBlock(context.Background(), origTds.DbStateWriter())
	if err != nil {
		t.Log("error while commit", err)
	}

	// Copy the state, modify both in-memory
	copy := orig.Copy()
	copyTds := origTds.Copy()
	origTds.StartNewBuffer()
	copyTds.StartNewBuffer()

	for i := byte(0); i < 255; i++ {
		origObj := orig.GetOrNewStateObject(common.BytesToAddress([]byte{i}))
		copyObj := copy.GetOrNewStateObject(common.BytesToAddress([]byte{i}))

		origObj.AddBalance(big.NewInt(2 * int64(i)))
		copyObj.AddBalance(big.NewInt(3 * int64(i)))

		err = origTds.TrieStateWriter().UpdateAccountData(context.Background(), origObj.address, &origObj.data, new(accounts.Account))
		if err != nil {
			t.Log(err)
		}
		err = copyTds.TrieStateWriter().UpdateAccountData(context.Background(), copyObj.address, &copyObj.data, new(accounts.Account))
		if err != nil {
			t.Log(err)
		}
	}
	// Finalise the changes on both concurrently
	done := make(chan struct{}, 1)
	go func() {
		ctx := context.WithValue(context.Background(), params.IsEIP158Enabled, true)
		err = orig.FinalizeTx(ctx, origTds.TrieStateWriter())
		if err != nil {
			t.Log("error while finalize", err)
		}

		_, err = origTds.ComputeTrieRoots(ctx)
		if err != nil {
			t.Log("error while ComputeTrieRoots", err)
		}

		origTds.SetBlockNr(ctx, 2)

		err = orig.CommitBlock(ctx, origTds.DbStateWriter())
		if err != nil {
			t.Log("error while commit", err)
		}

		close(done)
	}()

	ctx := context.WithValue(context.Background(), params.IsEIP158Enabled, true)

	err = copy.FinalizeTx(ctx, copyTds.TrieStateWriter())
	if err != nil {
		t.Log("error while finalize", err)
	}

	_, err = copyTds.ComputeTrieRoots(ctx)
	if err != nil {
		t.Log("error while ComputeTrieRoots", err)
	}

	copyTds.SetBlockNr(ctx, 2)
	err = copy.CommitBlock(ctx, copyTds.DbStateWriter())
	if err != nil {
		t.Log("error while commit", err)
	}
	<-done

	// Verify that the two states have been updated independently
	for i := byte(0); i < 255; i++ {
		origObj := orig.GetOrNewStateObject(common.BytesToAddress([]byte{i}))
		copyObj := copy.GetOrNewStateObject(common.BytesToAddress([]byte{i}))

		if want := big.NewInt(3 * int64(i)); origObj.Balance().Cmp(want) != 0 {
			t.Errorf("orig obj %d: balance mismatch: have %v, want %v", i, origObj.Balance(), want)
		}
		if want := big.NewInt(4 * int64(i)); copyObj.Balance().Cmp(want) != 0 {
			t.Errorf("copy obj %d: balance mismatch: have %v, want %v", i, copyObj.Balance(), want)
		}
	}
}
