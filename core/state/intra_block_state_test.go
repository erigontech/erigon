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
	check "gopkg.in/check.v1"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Tests that updating a state trie does not leak any database writes prior to
// actually committing the state.
func TestUpdateLeaks(t *testing.T) {
	// Create an empty state database
	db := ethdb.NewMemDatabase()
	tds := NewTrieDbState(common.Hash{}, db, 0)
	state := New(tds)

	// Update it with some accounts
	for i := byte(0); i < 255; i++ {
		tds.StartNewBuffer()
		addr := common.BytesToAddress([]byte{i})
		state.AddBalance(addr, uint256.NewInt().SetUint64(uint64(11*i)))
		state.SetNonce(addr, uint64(42*i))
		if i%2 == 0 {
			val := uint256.NewInt().SetBytes([]byte{i, i, i, i})
			state.SetState(addr, &common.Hash{i, i, i}, *val)
		}
		if i%3 == 0 {
			state.SetCode(addr, []byte{i, i, i, i, i})
		}
		_ = state.FinalizeTx(context.Background(), tds.TrieStateWriter())
	}

	_, err := tds.ComputeTrieRoots()
	if err != nil {
		t.Fatal("error while ComputeTrieRoots", err)
	}

	// Ensure that no data was leaked into the database
	keys, err := db.Keys()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(keys); i += 2 {
		if bytes.Equal(keys[i], dbutils.PreimagePrefix) {
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
	transTds := NewTrieDbState(common.Hash{}, transDb, 0)
	transState := New(transTds)
	transTds.StartNewBuffer()
	finalTds := NewTrieDbState(common.Hash{}, finalDb, 0)
	finalState := New(finalTds)
	finalTds.StartNewBuffer()

	modify := func(state *IntraBlockState, addr common.Address, i, tweak byte) {
		state.SetBalance(addr, uint256.NewInt().SetUint64(uint64(11*i+tweak)))
		state.SetNonce(addr, uint64(42*i+tweak))
		if i%2 == 0 {
			val := uint256.NewInt()
			state.SetState(addr, &common.Hash{i, i, i, 0}, *val)
			val.SetBytes([]byte{i, i, i, i, tweak})
			state.SetState(addr, &common.Hash{i, i, i, tweak}, *val)
		}
		if i%3 == 0 {
			state.SetCode(addr, []byte{i, i, i, i, i, tweak})
		}
	}

	// Modify the transient state.
	for i := byte(0); i < 255; i++ {
		modify(transState, common.Address{i}, i, 0)
	}

	// Write modifications to trie.
	err := transState.FinalizeTx(context.Background(), transTds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalizing state", err)
	}

	transTds.StartNewBuffer()

	// Overwrite all the data with new values in the transient database.
	for i := byte(0); i < 255; i++ {
		modify(transState, common.Address{i}, i, 99)
		modify(finalState, common.Address{i}, i, 99)
	}

	// Commit and cross check the databases.
	err = transState.FinalizeTx(context.Background(), transTds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalizing state", err)
	}

	_, err = transTds.ComputeTrieRoots()
	if err != nil {
		t.Fatal("error while ComputeTrieRoots", err)
	}

	transTds.SetBlockNr(1)

	err = transState.CommitBlock(context.Background(), transTds.DbStateWriter())
	if err != nil {
		t.Fatal("failed to commit transition state", err)
	}

	err = finalState.FinalizeTx(context.Background(), finalTds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalizing state", err)
	}

	_, err = finalTds.ComputeTrieRoots()
	if err != nil {
		t.Fatal("error while ComputeTrieRoots", err)
	}

	finalTds.SetBlockNr(1)
	if err := finalState.CommitBlock(context.Background(), finalTds.DbStateWriter()); err != nil {
		t.Fatalf("failed to commit final state: %v", err)
	}
	finalKeys, err := finalDb.Keys()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(finalKeys); i += 2 {
		if bytes.Equal(finalKeys[i], dbutils.PreimagePrefix) {
			continue
		}
		if _, err := transDb.Get(finalKeys[i], finalKeys[i+1]); err != nil {
			val, _ := finalDb.Get(finalKeys[i], finalKeys[i+1])
			t.Errorf("entry missing from the transition database: %x:%x -> %x", finalKeys[i], finalKeys[i+1], val)
		}
	}
	transKeys, err := transDb.Keys()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(transKeys); i += 2 {
		if bytes.Equal(transKeys[i], dbutils.PreimagePrefix) {
			continue
		}
		if _, err := finalDb.Get(transKeys[i], transKeys[i+1]); err != nil {
			val, _ := transDb.Get(transKeys[i], transKeys[i+1])
			t.Errorf("entry missing in the transition database: %x:%x -> %x", transKeys[i], transKeys[i+1], val)
		}
	}
}

func TestSnapshotRandom(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: mining")
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
				s.SetBalance(addr, uint256.NewInt().SetUint64(uint64(a.args[0])))
			},
			args: make([]int64, 1),
		},
		{
			name: "AddBalance",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddBalance(addr, uint256.NewInt().SetUint64(uint64(a.args[0])))
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
				val := uint256.NewInt().SetUint64(uint64(a.args[1]))
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
	var (
		db           = ethdb.NewMemDatabase()
		ds           = NewDbState(db.AbstractKV(), 0)
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
		checkds := NewDbState(db.AbstractKV(), 0)
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
		checkeqBigInt := func(op string, a, b *big.Int) bool {
			if err == nil && a.Cmp(b) != 0 {
				err = fmt.Errorf("got %s(%s) == %d, want %d", op, addr.Hex(), a, b)
				return false
			}
			return true
		}
		// Check basic accessor methods.
		checkeq("Exist", state.Exist(addr), checkstate.Exist(addr))
		checkeq("HasSuicided", state.HasSuicided(addr), checkstate.HasSuicided(addr))
		checkeqBigInt("GetBalance", state.GetBalance(addr).ToBig(), checkstate.GetBalance(addr).ToBig())
		checkeq("GetNonce", state.GetNonce(addr), checkstate.GetNonce(addr))
		checkeq("GetCode", state.GetCode(addr), checkstate.GetCode(addr))
		checkeq("GetCodeHash", state.GetCodeHash(addr), checkstate.GetCodeHash(addr))
		checkeq("GetCodeSize", state.GetCodeSize(addr), checkstate.GetCodeSize(addr))
		// Check storage.
		if obj := state.getStateObject(addr); obj != nil {
			err = ds.ForEachStorage(addr, []byte{} /*startKey*/, func(key, seckey common.Hash, value uint256.Int) bool {
				var out uint256.Int
				checkstate.GetState(addr, &key, &out)
				return checkeq("GetState("+key.Hex()+")", out, value)
			}, 1000)
			if err != nil {
				return err
			}
			err = checkds.ForEachStorage(addr, []byte{} /*startKey*/, func(key, seckey common.Hash, value uint256.Int) bool {
				var out uint256.Int
				state.GetState(addr, &key, &out)
				return checkeq("GetState("+key.Hex()+")", out, value)
			}, 1000)
			if err != nil {
				return err
			}
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

	_, err = s.tds.ComputeTrieRoots()
	if err != nil {
		c.Fatal("error while ComputeTrieRoots", err)
	}

	s.tds.SetBlockNr(1)

	err = s.state.CommitBlock(context.Background(), s.tds.DbStateWriter())
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
