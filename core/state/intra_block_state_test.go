// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

//go:build integration

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
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	stateLib "github.com/erigontech/erigon-lib/state"

	"github.com/erigontech/erigon/core/blockstm"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
)

func TestSnapshotRandom(t *testing.T) {
	t.Parallel()
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
	addrs     []libcommon.Address // all account addresses
	actions   []testAction        // modifications to the state
	snapshots []int               // actions indexes at which snapshot is taken
	err       error               // failure details are reported through this field
}

type testAction struct {
	name   string
	fn     func(testAction, *IntraBlockState)
	args   []int64
	noAddr bool
}

// newTestAction creates a random action that changes state.
func newTestAction(addr libcommon.Address, r *rand.Rand) testAction {
	actions := []testAction{
		{
			name: "SetBalance",
			fn: func(a testAction, s *IntraBlockState) {
				s.SetBalance(addr, uint256.NewInt(uint64(a.args[0])), tracing.BalanceChangeUnspecified)
			},
			args: make([]int64, 1),
		},
		{
			name: "AddBalance",
			fn: func(a testAction, s *IntraBlockState) {
				s.AddBalance(addr, uint256.NewInt(uint64(a.args[0])), tracing.BalanceChangeUnspecified)
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
				var key libcommon.Hash
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
			name: "Selfdestruct",
			fn: func(a testAction, s *IntraBlockState) {
				s.Selfdestruct(addr)
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
					libcommon.Hash{byte(a.args[0])})
			},
			args: make([]int64, 1),
		},
		{
			name: "SetTransientState",
			fn: func(a testAction, s *IntraBlockState) {
				var key libcommon.Hash
				binary.BigEndian.PutUint16(key[:], uint16(a.args[0]))
				val := uint256.NewInt(uint64(a.args[1]))
				s.SetTransientState(addr, key, *val)
			},
			args: make([]int64, 2),
		},
	}
	action := actions[r.Intn(len(actions))]
	var nameargs []string //nolint:prealloc
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
	addrs := make([]libcommon.Address, 50)
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
	db := memdb.NewStateDB("")
	defer db.Close()

	agg, err := stateLib.NewAggregator(context.Background(), datadir.New(""), 16, db, log.New())
	if err != nil {
		test.err = err
		return false
	}
	defer agg.Close()

	_db, err := temporal.New(db, agg)
	if err != nil {
		test.err = err
		return false
	}

	tx, err := _db.BeginTemporalRw(context.Background()) //nolint:gocritic
	if err != nil {
		test.err = err
		return false
	}
	defer tx.Rollback()

	domains, err := stateLib.NewSharedDomains(tx, log.New())
	if err != nil {
		test.err = err
		return false
	}
	defer domains.Close()

	domains.SetTxNum(1)
	domains.SetBlockNum(1)
	err = rawdbv3.TxNums.Append(tx, 1, 1)
	if err != nil {
		test.err = err
		return false
	}
	var (
		state        = New(NewReaderV3(domains))
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
		checkstate := New(NewReaderV3(domains))
		for _, action := range test.actions[:test.snapshots[sindex]] {
			action.fn(action, checkstate)
		}
		state.RevertToSnapshot(snapshotRevs[sindex])
		if err := test.checkEqual(state, checkstate); err != nil {
			test.err = fmt.Errorf("state mismatch after revert to snapshot %d\n%w", sindex, err)
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
		checkeq("HasSelfdestructed", state.HasSelfdestructed(addr), checkstate.HasSelfdestructed(addr))
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
	if !reflect.DeepEqual(state.GetRawLogs(0), checkstate.GetRawLogs(0)) {
		return fmt.Errorf("got GetRawLogs(libcommon.Hash{}) == %v, want GetRawLogs(libcommon.Hash{}) == %v",
			state.GetRawLogs(0), checkstate.GetRawLogs(0))
	}
	return nil
}

func TestTransientStorage(t *testing.T) {
	t.Parallel()
	state := New(nil)

	key := libcommon.Hash{0x01}
	value := uint256.NewInt(2)
	addr := libcommon.Address{}

	state.SetTransientState(addr, key, *value)
	if exp, got := 1, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}
	// the retrieved value should equal what was set
	if got := state.GetTransientState(addr, key); got != *value {
		t.Fatalf("transient storage mismatch: have %x, want %x", got, value)
	}

	// revert the transient state being set and then check that the
	// value is now the empty hash
	state.journal.revert(state, 0)
	if got, exp := state.GetTransientState(addr, key), (*uint256.NewInt(0)); exp != got {
		t.Fatalf("transient storage mismatch: have %x, want %x", got, exp)
	}
}

func TestMVHashMapReadWriteDelete(t *testing.T) {
	t.Parallel()

	db := memdb.New("")
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	assert.NoError(t, err)
	ds := NewPlainState(tx, 1, nil)
	tsw := NewPlainStateWriter(tx, nil, 1)
	mvhm := blockstm.MakeMVHashMap()
	s := NewWithMVHashmap(ds, mvhm)

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := s.Copy()
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := libcommon.HexToAddress("0x01")
	key := libcommon.HexToHash("0x01")
	val := *uint256.NewInt(1)
	balance := uint256.NewInt(100)

	var v uint256.Int

	// Tx0 read
	states[0].GetState(addr, &key, &v)

	assert.Equal(t, *uint256.NewInt(0), v)

	// Tx1 write
	states[1].GetOrNewStateObject(addr)
	states[1].SetState(addr, &key, val)
	states[1].SetBalance(addr, balance)
	states[1].FlushMVWriteSet()

	// Tx1 read
	states[1].GetState(addr, &key, &v)
	b := states[1].GetBalance(addr)

	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)

	// Tx2 read
	states[2].GetState(addr, &key, &v)
	b = states[2].GetBalance(addr)

	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)

	// Tx3 delete
	states[3].Selfdestruct(addr)

	// Within Tx 3, the state should not change before finalize
	states[3].GetState(addr, &key, &v)
	assert.Equal(t, val, v)

	// After finalizing Tx 3, the state will change
	states[3].FinalizeTx(&chain.Rules{}, tsw)
	states[3].GetState(addr, &key, &v)
	assert.Equal(t, *uint256.NewInt(0), v)
	states[3].FlushMVWriteSet()

	// Tx4 read
	states[4].GetState(addr, &key, &v)
	b = states[4].GetBalance(addr)

	assert.Equal(t, *uint256.NewInt(0), v)
	assert.Equal(t, uint256.NewInt(0), b)
}

func TestMVHashMapRevert(t *testing.T) {
	t.Parallel()

	db := memdb.New("")
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	assert.NoError(t, err)
	ds := NewPlainState(tx, 1, nil)
	tsw := NewPlainStateWriter(tx, nil, 1)
	mvhm := blockstm.MakeMVHashMap()
	s := NewWithMVHashmap(ds, mvhm)

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := s.Copy()
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := libcommon.HexToAddress("0x01")
	key := libcommon.HexToHash("0x01")
	val := *uint256.NewInt(1)
	balance := uint256.NewInt(100)

	// Tx0 write
	states[0].GetOrNewStateObject(addr)
	states[0].SetState(addr, &key, val)
	states[0].SetBalance(addr, balance)
	states[0].FlushMVWriteSet()

	var v uint256.Int

	// Tx1 perform some ops and then revert
	snapshot := states[1].Snapshot()
	states[1].AddBalance(addr, uint256.NewInt(100))
	states[1].SetState(addr, &key, *uint256.NewInt(1))
	states[1].GetState(addr, &key, &v)
	b := states[1].GetBalance(addr)
	assert.Equal(t, uint256.NewInt(200), b)
	assert.Equal(t, *uint256.NewInt(1), v)

	states[1].Selfdestruct(addr)

	states[1].RevertToSnapshot(snapshot)

	states[1].GetState(addr, &key, &v)
	b = states[1].GetBalance(addr)

	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)
	states[1].FinalizeTx(&chain.Rules{}, tsw)
	states[1].FlushMVWriteSet()

	// Tx2 check the state and balance
	states[2].GetState(addr, &key, &v)
	b = states[2].GetBalance(addr)

	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)
}

func TestMVHashMapMarkEstimate(t *testing.T) {
	t.Parallel()

	db := memdb.New("")
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	assert.NoError(t, err)
	ds := NewPlainState(tx, 1, nil)
	mvhm := blockstm.MakeMVHashMap()
	s := NewWithMVHashmap(ds, mvhm)

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := s.Copy()
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := libcommon.HexToAddress("0x01")
	key := libcommon.HexToHash("0x01")
	val := *uint256.NewInt(1)
	balance := uint256.NewInt(100)

	var v uint256.Int

	// Tx0 read
	states[0].GetState(addr, &key, &v)
	assert.Equal(t, *uint256.NewInt(0), v)

	// Tx0 write
	states[0].SetState(addr, &key, val)
	states[0].GetState(addr, &key, &v)
	assert.Equal(t, val, v)
	states[0].FlushMVWriteSet()

	// Tx1 write
	states[1].GetOrNewStateObject(addr)
	states[1].SetState(addr, &key, val)
	states[1].SetBalance(addr, balance)
	states[1].FlushMVWriteSet()

	// Tx2 read
	states[2].GetState(addr, &key, &v)
	b := states[2].GetBalance(addr)

	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)

	// Tx1 mark estimate
	for _, v := range states[1].MVWriteList() {
		mvhm.MarkEstimate(v.Path, 1)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		} else {
			t.Log("Recovered in f", r)
		}
	}()

	// Tx2 read again should get default (empty) vals because its dependency Tx1 is marked as estimate
	states[2].GetState(addr, &key, &v)
	states[2].GetBalance(addr)

	// Tx1 read again should get Tx0 vals
	states[1].GetState(addr, &key, &v)
	assert.Equal(t, val, v)
}

func TestMVHashMapOverwrite(t *testing.T) {
	t.Parallel()

	db := memdb.New("")
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	assert.NoError(t, err)
	ds := NewPlainState(tx, 1, nil)
	mvhm := blockstm.MakeMVHashMap()
	s := NewWithMVHashmap(ds, mvhm)

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := s.Copy()
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := libcommon.HexToAddress("0x01")
	key := libcommon.HexToHash("0x01")
	val1 := *uint256.NewInt(1)
	balance1 := uint256.NewInt(100)
	val2 := *uint256.NewInt(2)
	balance2 := uint256.NewInt(200)

	var v uint256.Int

	// Tx0 write
	states[0].GetOrNewStateObject(addr)
	states[0].SetState(addr, &key, val1)
	states[0].SetBalance(addr, balance1)
	states[0].FlushMVWriteSet()

	// Tx1 write
	states[1].SetState(addr, &key, val2)
	states[1].SetBalance(addr, balance2)
	states[1].GetState(addr, &key, &v)
	b := states[1].GetBalance(addr)
	states[1].FlushMVWriteSet()

	assert.Equal(t, val2, v)
	assert.Equal(t, balance2, b)

	// Tx2 read should get Tx1's value
	states[2].GetState(addr, &key, &v)
	b = states[2].GetBalance(addr)

	assert.Equal(t, val2, v)
	assert.Equal(t, balance2, b)

	// Tx1 delete
	for _, v := range states[1].writeMap {
		mvhm.Delete(v.Path, 1)

		states[1].writeMap = nil
	}

	// Tx2 read should get Tx0's value
	states[2].GetState(addr, &key, &v)
	b = states[2].GetBalance(addr)

	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, b)

	// Tx1 read should get Tx0's value
	states[1].GetState(addr, &key, &v)
	b = states[1].GetBalance(addr)

	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, b)

	// Tx0 delete
	for _, v := range states[0].writeMap {
		mvhm.Delete(v.Path, 0)

		states[0].writeMap = nil
	}

	// Tx2 read again should get default vals
	states[2].GetState(addr, &key, &v)
	b = states[2].GetBalance(addr)

	assert.Equal(t, *uint256.NewInt(0), v)
	assert.Equal(t, uint256.NewInt(0), b)
}

func TestMVHashMapWriteNoConflict(t *testing.T) {
	t.Parallel()

	db := memdb.New("")
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	assert.NoError(t, err)
	ds := NewPlainState(tx, 1, nil)
	mvhm := blockstm.MakeMVHashMap()
	s := NewWithMVHashmap(ds, mvhm)

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := s.Copy()
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := libcommon.HexToAddress("0x01")
	key1 := libcommon.HexToHash("0x01")
	key2 := libcommon.HexToHash("0x02")
	val1 := *uint256.NewInt(1)
	balance1 := uint256.NewInt(100)
	val2 := *uint256.NewInt(2)

	// Tx0 write
	states[0].GetOrNewStateObject(addr)
	states[0].FlushMVWriteSet()

	// Tx2 write
	states[2].SetState(addr, &key2, val2)
	states[2].FlushMVWriteSet()

	// Tx1 write
	tx1Snapshot := states[1].Snapshot()
	states[1].SetState(addr, &key1, val1)
	states[1].SetBalance(addr, balance1)
	states[1].FlushMVWriteSet()

	var v uint256.Int

	// Tx1 read
	states[1].GetState(addr, &key1, &v)
	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, states[1].GetBalance(addr))
	// Tx1 should see empty value in key2
	states[1].GetState(addr, &key2, &v)
	assert.Equal(t, *uint256.NewInt(0), v)

	// Tx2 read
	states[2].GetState(addr, &key2, &v)
	assert.Equal(t, val2, v)
	// Tx2 should see values written by Tx1
	states[2].GetState(addr, &key1, &v)
	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, states[2].GetBalance(addr))

	// Tx3 read
	states[3].GetState(addr, &key1, &v)
	assert.Equal(t, val1, v)
	states[3].GetState(addr, &key2, &v)
	assert.Equal(t, val2, v)
	assert.Equal(t, balance1, states[3].GetBalance(addr))

	// Tx2 delete
	for _, v := range states[2].writeMap {
		mvhm.Delete(v.Path, 2)

		states[2].writeMap = nil
	}

	// Tx3 read
	states[3].GetState(addr, &key1, &v)
	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, states[3].GetBalance(addr))
	// Tx3 should see empty value in key2
	states[3].GetState(addr, &key2, &v)
	assert.Equal(t, *uint256.NewInt(0), v)

	// Tx1 revert
	states[1].RevertToSnapshot(tx1Snapshot)
	states[1].FlushMVWriteSet()

	// Tx3 read
	states[3].GetState(addr, &key1, &v)
	assert.Equal(t, *uint256.NewInt(0), v)
	states[3].GetState(addr, &key2, &v)
	assert.Equal(t, *uint256.NewInt(0), v)
	assert.Equal(t, uint256.NewInt(0), states[3].GetBalance(addr))

	// Tx1 delete
	for _, v := range states[1].writeMap {
		mvhm.Delete(v.Path, 1)

		states[1].writeMap = nil
	}

	// Tx3 read
	states[3].GetState(addr, &key1, &v)
	assert.Equal(t, *uint256.NewInt(0), v)
	states[3].GetState(addr, &key2, &v)
	assert.Equal(t, *uint256.NewInt(0), v)
	assert.Equal(t, uint256.NewInt(0), states[3].GetBalance(addr))
}

func TestApplyMVWriteSet(t *testing.T) {
	t.Parallel()

	db := memdb.New("")
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	assert.NoError(t, err)
	ds := NewPlainState(tx, 1, nil)
	tsw := NewPlainStateWriter(tx, nil, 1)
	mvhm := blockstm.MakeMVHashMap()
	s := NewWithMVHashmap(ds, mvhm)

	sClean := s.Copy()
	sClean.mvHashmap = nil

	sSingleProcess := sClean.Copy()

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := s.Copy()
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr1 := libcommon.HexToAddress("0x01")
	addr2 := libcommon.HexToAddress("0x02")
	addr3 := libcommon.HexToAddress("0x03")
	key1 := libcommon.HexToHash("0x01")
	key2 := libcommon.HexToHash("0x02")
	val1 := *uint256.NewInt(1)
	balance1 := uint256.NewInt(100)
	val2 := *uint256.NewInt(2)
	balance2 := uint256.NewInt(200)
	code := []byte{1, 2, 3}

	// Tx0 write
	states[0].GetOrNewStateObject(addr1)
	states[0].SetState(addr1, &key1, val1)
	states[0].SetBalance(addr1, balance1)
	states[0].SetState(addr2, &key2, val2)
	states[0].GetOrNewStateObject(addr3)
	states[0].FinalizeTx(&chain.Rules{}, tsw)
	states[0].FlushMVWriteSet()

	sSingleProcess.GetOrNewStateObject(addr1)
	sSingleProcess.SetState(addr1, &key1, val1)
	sSingleProcess.SetBalance(addr1, balance1)
	sSingleProcess.SetState(addr2, &key2, val2)
	sSingleProcess.GetOrNewStateObject(addr3)

	sClean.ApplyMVWriteSet(states[0].MVWriteList())

	// Tx1 write
	states[1].SetState(addr1, &key2, val2)
	states[1].SetBalance(addr1, balance2)
	states[1].SetNonce(addr1, 1)
	states[1].FinalizeTx(&chain.Rules{}, tsw)
	states[1].FlushMVWriteSet()

	sSingleProcess.SetState(addr1, &key2, val2)
	sSingleProcess.SetBalance(addr1, balance2)
	sSingleProcess.SetNonce(addr1, 1)

	sClean.ApplyMVWriteSet(states[1].MVWriteList())

	// Tx2 write
	states[2].SetState(addr1, &key1, val2)
	states[2].SetBalance(addr1, balance2)
	states[2].SetNonce(addr1, 2)
	states[2].FinalizeTx(&chain.Rules{}, tsw)
	states[2].FlushMVWriteSet()

	sSingleProcess.SetState(addr1, &key1, val2)
	sSingleProcess.SetBalance(addr1, balance2)
	sSingleProcess.SetNonce(addr1, 2)

	sClean.ApplyMVWriteSet(states[2].MVWriteList())

	// Tx3 write
	states[3].Selfdestruct(addr2)
	states[3].SetCode(addr1, code)
	states[3].FinalizeTx(&chain.Rules{}, tsw)
	states[3].FlushMVWriteSet()

	sSingleProcess.Selfdestruct(addr2)
	sSingleProcess.SetCode(addr1, code)

	sClean.ApplyMVWriteSet(states[3].MVWriteList())
}
