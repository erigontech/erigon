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

package state

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSnapshotRandom(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	config := &quick.Config{MaxCount: 10}
	ts := &snapshotTest{}
	err := quick.Check(func() bool {
		return ts.run(t)
	}, config)
	if cerr, ok := errors.AsType[*quick.CheckError](err); ok {
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
	addrs     []accounts.Address // all account addresses
	actions   []testAction       // modifications to the state
	snapshots []int              // actions indexes at which snapshot is taken
	err       error              // failure details are reported through this field
}

type testAction struct {
	name   string
	fn     func(testAction, *IntraBlockState) error
	args   []int64
	noAddr bool
}

// newTestAction creates a random action that changes state.
func newTestAction(addr accounts.Address, r *rand.Rand) testAction {
	actions := []testAction{
		{
			name: "SetBalance",
			fn: func(a testAction, s *IntraBlockState) error {
				return s.SetBalance(addr, u256.U64(uint64(a.args[0])), tracing.BalanceChangeUnspecified)
			},
			args: make([]int64, 1),
		},
		{
			name: "AddBalance",
			fn: func(a testAction, s *IntraBlockState) error {
				return s.AddBalance(addr, u256.U64(uint64(a.args[0])), tracing.BalanceChangeUnspecified)
			},
			args: make([]int64, 1),
		},
		{
			name: "SetNonce",
			fn: func(a testAction, s *IntraBlockState) error {
				return s.SetNonce(addr, uint64(a.args[0]), tracing.NonceChangeUnspecified)
			},
			args: make([]int64, 1),
		},
		{
			name: "SetState",
			fn: func(a testAction, s *IntraBlockState) error {
				var key common.Hash
				binary.BigEndian.PutUint16(key[:], uint16(a.args[0]))
				val := uint256.NewInt(uint64(a.args[1]))
				return s.SetState(addr, accounts.InternKey(key), *val)
			},
			args: make([]int64, 2),
		},
		{
			name: "SetCode",
			fn: func(a testAction, s *IntraBlockState) error {
				code := make([]byte, 16)
				binary.BigEndian.PutUint64(code, uint64(a.args[0]))
				binary.BigEndian.PutUint64(code[8:], uint64(a.args[1]))
				return s.SetCode(addr, code, tracing.CodeChangeUnspecified)
			},
			args: make([]int64, 2),
		},
		{
			name: "CreateAccount",
			fn: func(a testAction, s *IntraBlockState) error {
				return s.CreateAccount(addr, true)
			},
		},
		{
			name: "Selfdestruct",
			fn: func(a testAction, s *IntraBlockState) error {
				_, err := s.Selfdestruct(addr, false)
				return err
			},
		},
		{
			name: "AddRefund",
			fn: func(a testAction, s *IntraBlockState) error {
				s.AddRefund(uint64(a.args[0]))
				return nil
			},
			args:   make([]int64, 1),
			noAddr: true,
		},
		{
			name: "AddLog",
			fn: func(a testAction, s *IntraBlockState) error {
				data := make([]byte, 2)
				binary.BigEndian.PutUint16(data, uint16(a.args[0]))
				s.AddLog(&types.Log{Address: addr.Value(), Data: data})
				return nil
			},
			args: make([]int64, 1),
		},
		{
			name: "AddAddressToAccessList",
			fn: func(a testAction, s *IntraBlockState) error {
				s.AddAddressToAccessList(addr)
				return nil
			},
		},
		{
			name: "AddSlotToAccessList",
			fn: func(a testAction, s *IntraBlockState) error {
				s.AddSlotToAccessList(addr,
					accounts.InternKey(common.Hash{byte(a.args[0])}))
				return nil
			},
			args: make([]int64, 1),
		},
		{
			name: "SetTransientState",
			fn: func(a testAction, s *IntraBlockState) error {
				var key common.Hash
				binary.BigEndian.PutUint16(key[:], uint16(a.args[0]))
				val := uint256.NewInt(uint64(a.args[1]))
				s.SetTransientState(addr, accounts.InternKey(key), *val)
				return nil
			},
			args: make([]int64, 2),
		},
	}
	action := actions[r.Intn(len(actions))]
	var nameargs []string //nolint:prealloc
	if !action.noAddr {
		nameargs = append(nameargs, addr.String())
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
	addrs := make([]accounts.Address, 50)
	for i := range addrs {
		addrs[i] = accounts.InternAddress(common.Address{byte(i)})
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

func (test *snapshotTest) run(t *testing.T) bool {
	_, tx, _ := NewTestRwTx(t)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	if err != nil {
		test.err = err
		return false
	}
	var (
		state        = New(NewReaderV3(execctx.NewTemporalTxStateGetter(tx)))
		snapshotRevs = make([]int, len(test.snapshots))
		sindex       = 0
	)
	defer state.Close()
	for i, action := range test.actions {
		if len(test.snapshots) > sindex && i == test.snapshots[sindex] {
			snapshotRevs[sindex] = state.PushSnapshot()
			sindex++
		}
		if err := action.fn(action, state); err != nil {
			test.err = err
			return false
		}
	}
	// Revert all snapshots in reverse order. Each revert must yield a state
	// that is equivalent to fresh state with all actions up the snapshot applied.
	for sindex--; sindex >= 0; sindex-- {
		checkstate := New(NewReaderV3(execctx.NewTemporalTxStateGetter(tx)))
		for _, action := range test.actions[:test.snapshots[sindex]] {
			if err := action.fn(action, checkstate); err != nil {
				test.err = err
				checkstate.Close()
				return false
			}
		}
		state.RevertToSnapshot(snapshotRevs[sindex], nil)
		state.PopSnapshot(snapshotRevs[sindex])
		err := test.checkEqual(state, checkstate)
		checkstate.Close()
		if err != nil {
			test.err = fmt.Errorf("state mismatch after revert to snapshot %d\n%w", sindex, err)
			return false
		}
	}
	return true
}

// checkEqual checks that methods of state and checkstate return the same values.
func (test *snapshotTest) checkEqual(state, checkstate *IntraBlockState) error {
	for _, addr := range test.addrs {
		var err error
		checkeq := func(op string, a, b any) bool {
			if err == nil && !reflect.DeepEqual(a, b) {
				err = fmt.Errorf("got %s(%s) == %v, want %v", op, addr, a, b)
				return false
			}
			return true
		}
		checkeqBigInt := func(op string, a, b *big.Int) bool {
			if err == nil && a.Cmp(b) != 0 {
				err = fmt.Errorf("got %s(%s) == %d, want %d", op, addr, a, b)
				return false
			}
			return true
		}
		// Check basic accessor methods.
		se, err := state.Exist(addr)
		if err != nil {
			return err
		}
		ce, err := checkstate.Exist(addr)
		if err != nil {
			return err
		}
		if !checkeq("Exist", se, ce) {
			return err
		}
		ssd, err := state.HasSelfdestructed(addr)
		if err != nil {
			return err
		}
		csd, err := checkstate.HasSelfdestructed(addr)
		if err != nil {
			return err
		}
		checkeq("HasSelfdestructed", ssd, csd)
		sb, err := state.GetBalance(addr)
		if err != nil {
			return err
		}
		cb, err := checkstate.GetBalance(addr)
		if err != nil {
			return err
		}
		checkeqBigInt("GetBalance", sb.ToBig(), cb.ToBig())
		sn, err := state.GetNonce(addr)
		if err != nil {
			return err
		}
		cn, err := checkstate.GetNonce(addr)
		if err != nil {
			return err
		}
		checkeq("GetNonce", sn, cn)
		sc, err := state.GetCode(addr)
		if err != nil {
			return err
		}
		cc, err := checkstate.GetCode(addr)
		if err != nil {
			return err
		}
		checkeq("GetCode", sc, cc)
		sch, err := state.GetCodeHash(addr)
		if err != nil {
			return err
		}
		cch, err := checkstate.GetCodeHash(addr)
		if err != nil {
			return err
		}
		checkeq("GetCodeHash", sch, cch)
		scs, err := state.GetCodeSize(addr)
		if err != nil {
			return err
		}
		ccs, err := checkstate.GetCodeSize(addr)
		if err != nil {
			return err
		}
		checkeq("GetCodeSize", scs, ccs)
		// Check storage.
		obj, err := state.getStateObject(addr, true)
		if err != nil {
			return err
		}
		if obj != nil {
			for key, value := range obj.dirtyStorage {
				out, _ := checkstate.GetState(addr, key)
				if !checkeq("GetState("+key.String()+")", out, value) {
					return err
				}
			}
		}
		obj, err = checkstate.getStateObject(addr, true)
		if err != nil {
			return err
		}
		if obj != nil {
			for key, value := range obj.dirtyStorage {
				out, _ := state.GetState(addr, key)
				if !checkeq("GetState("+key.String()+")", out, value) {
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
		return fmt.Errorf("got GetRawLogs(common.Hash{}) == %v, want GetRawLogs(common.Hash{}) == %v",
			state.GetRawLogs(0), checkstate.GetRawLogs(0))
	}
	return nil
}

func TestTransientStorage(t *testing.T) {
	t.Parallel()
	state := New(nil)
	defer state.Close()

	key := accounts.InternKey(common.Hash{0x01})
	value := uint256.NewInt(2)
	addr := accounts.Address{}

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
	if got, exp := state.GetTransientState(addr, key), (uint256.Int{}); exp != got {
		t.Fatalf("transient storage mismatch: have %x, want %x", got, exp)
	}
}

func TestCloseResetsRevisions(t *testing.T) {
	t.Parallel()
	state := New(nil)
	t.Cleanup(state.Close)

	state.PushSnapshot()
	require.NotEmpty(t, state.revisions.valid)

	state.Close()
	require.Empty(t, state.revisions.valid)
	require.Zero(t, state.revisions.nextId)
}

func TestCloseReleasesVersionedWrites(t *testing.T) {
	t.Parallel()
	state := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(state.Close)
	state.SetNoMaterialize(true)
	state.SetTxContext(1, 0)

	require.NoError(t, state.TouchAccount(accounts.InternAddress([20]byte{0xe1})))
	require.NotZero(t, state.versionedWrites.Count())

	handedOut := state.VersionedWrites()
	state.Close()

	require.Zero(t, state.versionedWrites.Count())
	require.NotZero(t, handedOut.Count(), "the clone handed to callers must survive Close")
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	state := New(nil)

	state.PushSnapshot()
	state.Close()
	require.NotPanics(t, state.Close)
}

func TestVersionMapReadWriteDelete(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)

	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))

	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := NewWithVersionMap(reader, mvhm)
		defer sCopy.Close() //nolint:gocritic
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	val := u256.U64(1)
	balance := u256.U64(100)

	// Tx0 read
	v, err := states[0].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)

	// Tx1 write
	_, err = states[1].GetOrNewStateObject(addr)
	require.NoError(t, err)
	require.NoError(t, states[1].SetState(addr, key, val))
	require.NoError(t, states[1].SetBalance(addr, balance, tracing.BalanceChangeUnspecified))
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)

	// Tx1 read
	v, err = states[1].GetState(addr, key)
	assert.NoError(t, err)
	b, err := states[1].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)

	// Tx2 read
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)

	// Tx3 delete. FinalizeTx below is the materialized-object commit path
	// (serial/genesis/RPC); the parallel executor never runs it (see the
	// IsVersioned guards in txtask.go) and instead commits via the write-set.
	// Materialize explicitly here — as Tx1 does — so FinalizeTx's updateAccount
	// marks the object deleted and the post-finalize read observes the wipe.
	_, err = states[3].GetOrNewStateObject(addr)
	require.NoError(t, err)
	_, err = states[3].Selfdestruct(addr, false)
	require.NoError(t, err)

	// Within Tx 3, the state should not change before finalize
	v, err = states[3].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, val, v)

	// After finalizing Tx 3, the state will change
	require.NoError(t, states[3].FinalizeTx(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))
	v, err = states[3].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	states[3].versionMap.FlushVersionedWrites(states[3].VersionedWrites(), true)

	// Tx4 read
	v, err = states[4].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[4].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	assert.Equal(t, uint256.Int{}, b)
}

func TestVersionMapRevert(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)

	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := NewWithVersionMap(reader, mvhm)
		defer sCopy.Close() //nolint:gocritic
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	val := u256.U64(1)
	balance := u256.U64(100)

	// Tx0 write
	_, err := states[0].GetOrNewStateObject(addr)
	require.NoError(t, err)
	require.NoError(t, states[0].SetState(addr, key, val))
	require.NoError(t, states[0].SetBalance(addr, balance, tracing.BalanceChangeUnspecified))
	states[0].versionMap.FlushVersionedWrites(states[0].VersionedWrites(), true)

	// Tx1 perform some ops and then revert
	snapshot := states[1].PushSnapshot()
	require.NoError(t, states[1].AddBalance(addr, u256.U64(100), tracing.BalanceChangeUnspecified))
	require.NoError(t, states[1].SetState(addr, key, u256.U64(1)))
	v, err := states[1].GetState(addr, key)
	assert.NoError(t, err)
	b, err := states[1].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, u256.U64(200), b)
	assert.Equal(t, u256.U64(1), v)

	_, err = states[1].Selfdestruct(addr, false)
	require.NoError(t, err)

	states[1].RevertToSnapshot(snapshot, nil)
	states[1].PopSnapshot(snapshot)

	v, err = states[1].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[1].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)
	require.NoError(t, states[1].FinalizeTx(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)

	// Tx2 check the state and balance
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)
}

func TestVersionMapMarkEstimate(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()
	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := NewWithVersionMap(reader, mvhm)
		defer sCopy.Close() //nolint:gocritic
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	val := u256.U64(1)
	balance := u256.U64(100)

	// Tx0 read
	v, err := states[0].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)

	// Tx0 write
	require.NoError(t, states[0].SetState(addr, key, val))
	v, err = states[0].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
	states[0].versionMap.FlushVersionedWrites(states[0].VersionedWrites(), true)

	// Tx1 write
	_, err = states[1].GetOrNewStateObject(addr)
	require.NoError(t, err)
	require.NoError(t, states[1].SetState(addr, key, val))
	require.NoError(t, states[1].SetBalance(addr, balance, tracing.BalanceChangeUnspecified))
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)

	// Tx2 read
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	b, err := states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
	assert.Equal(t, balance, b)

	// Tx1 mark estimate
	for h := range states[1].VersionedWrites().AllHeaders() {
		mvhm.MarkEstimate(h.Address, h.Path, h.Key, 1)
	}

	// Read-once (Block-STM): states[2] already recorded its state/balance reads
	// above, so the repeat reads are served from the read-set and no longer abort
	// eagerly when Tx1's writes are marked ESTIMATE. The estimate dependency is
	// caught at commit — ValidateVersion re-reads Tx1's now-Estimate balance cell
	// (MVReadResultDependency) and returns VersionInvalid, which drives re-execution.
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, u256.U64(1), v)
	_, err = states[2].GetBalance(addr)
	assert.NoError(t, err)

	var io2 VersionedIO
	states[2].MergeTxIOInto(&io2, states[2].VersionedWrites())
	valid := mvhm.ValidateVersion(2, &io2, func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}, true, false, false, "")
	assert.Equal(t, VersionInvalid, valid, "commit-time validation catches the ESTIMATE dependency")

	// Tx1 read again should get Tx0 vals
	v, err = states[1].GetState(addr, key)
	assert.NoError(t, err)
	assert.Equal(t, val, v)
}

func TestVersionMapOverwrite(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := NewWithVersionMap(reader, mvhm)
		defer sCopy.Close() //nolint:gocritic
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	val1 := u256.U64(1)
	balance1 := u256.U64(100)
	val2 := u256.U64(2)
	balance2 := u256.U64(200)

	// Tx0 write
	_, err := states[0].GetOrNewStateObject(addr)
	require.NoError(t, err)
	require.NoError(t, states[0].SetState(addr, key, val1))
	require.NoError(t, states[0].SetBalance(addr, balance1, tracing.BalanceChangeUnspecified))
	states[0].versionMap.FlushVersionedWrites(states[0].VersionedWrites(), true)

	// Tx1 write
	require.NoError(t, states[1].SetState(addr, key, val2))
	require.NoError(t, states[1].SetBalance(addr, balance2, tracing.BalanceChangeUnspecified))
	v, err := states[1].GetState(addr, key)
	assert.NoError(t, err)
	b, err := states[1].GetBalance(addr)
	assert.NoError(t, err)
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)

	assert.Equal(t, val2, v)
	assert.Equal(t, balance2, b)

	// Tx2 read should get Tx1's value
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val2, v)
	assert.Equal(t, balance2, b)

	// Tx1 delete
	for h := range states[1].versionedWrites.AllHeaders() {
		mvhm.Delete(h.Address, h.Path, h.Key, 1, true)
	}
	states[1].versionedWrites = WriteSet{}

	// Tx2 read should get Tx0's value
	states[2].versionedReads = ReadSet{}
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, b)

	// Tx1 read should get Tx0's value
	v, err = states[1].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[1].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, val1, v)
	assert.Equal(t, balance1, b)

	// Tx0 delete
	for h := range states[0].versionedWrites.AllHeaders() {
		mvhm.Delete(h.Address, h.Path, h.Key, 0, true)
	}
	states[0].versionedWrites = WriteSet{}

	// Tx2 read again should get default vals
	states[2].versionedReads = ReadSet{}
	v, err = states[2].GetState(addr, key)
	assert.NoError(t, err)
	b, err = states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	assert.Equal(t, uint256.Int{}, b)
}

func TestVersionMapWriteNoConflict(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := NewWithVersionMap(reader, mvhm)
		defer sCopy.Close() //nolint:gocritic
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key1 := accounts.InternKey(common.HexToHash("0x01"))
	key2 := accounts.InternKey(common.HexToHash("0x02"))
	val1 := u256.U64(1)
	balance1 := u256.U64(100)
	val2 := u256.U64(2)

	// Tx0 write
	_, err := states[0].GetOrNewStateObject(addr)
	require.NoError(t, err)
	states[0].versionMap.FlushVersionedWrites(states[0].VersionedWrites(), true)

	// Tx2 write
	require.NoError(t, states[2].SetState(addr, key2, val2))
	states[2].versionMap.FlushVersionedWrites(states[2].VersionedWrites(), true)

	// Tx1 write
	tx1Snapshot := states[1].PushSnapshot()
	require.NoError(t, states[1].SetState(addr, key1, val1))
	require.NoError(t, states[1].SetBalance(addr, balance1, tracing.BalanceChangeUnspecified))
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)

	// Tx1 read
	v, err := states[1].GetState(addr, key1)
	assert.NoError(t, err)
	assert.Equal(t, val1, v)
	b, err := states[1].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, balance1, b)
	// Tx1 should see empty value in key2
	v, err = states[1].GetState(addr, key2)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)

	// Tx2 read
	// Clear cached reads and state objects from Tx2's SetState call above —
	// those reads were recorded when Tx1 hadn't flushed yet (Tx0's version).
	// Now that Tx1 has flushed, re-reading without stale cache simulates a
	// re-execution that the scheduler would trigger on dependency.
	states[2].stateObjects = map[accounts.Address]*stateObject{}
	states[2].versionedReads = ReadSet{}
	v, err = states[2].GetState(addr, key2)
	assert.NoError(t, err)
	assert.Equal(t, val2, v)
	// Tx2 should see values written by Tx1
	v, err = states[2].GetState(addr, key1)
	assert.NoError(t, err)
	assert.Equal(t, val1, v)
	b, err = states[2].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, balance1, b)

	// Tx3 read
	v, err = states[3].GetState(addr, key1)
	assert.NoError(t, err)
	assert.Equal(t, val1, v)
	v, err = states[3].GetState(addr, key2)
	assert.NoError(t, err)
	assert.Equal(t, val2, v)
	b, err = states[3].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, balance1, b)

	// Tx2 delete
	for h := range states[2].versionedWrites.AllHeaders() {
		mvhm.Delete(h.Address, h.Path, h.Key, 2, true)
	}
	states[2].versionedWrites = WriteSet{}

	// Tx3 read
	states[3].versionedReads = ReadSet{}
	v, err = states[3].GetState(addr, key1)
	assert.NoError(t, err)
	assert.Equal(t, val1, v)
	b, err = states[3].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, balance1, b)
	// Tx3 should see empty value in key2
	v, err = states[3].GetState(addr, key2)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)

	// Tx1 revert
	states[1].RevertToSnapshot(tx1Snapshot, nil)
	states[1].PopSnapshot(tx1Snapshot)
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)
	// map deletes necessary here as they happen in scheduler not ibs
	states[1].versionMap.Delete(addr, StoragePath, key1, 1, true)
	states[1].versionMap.Delete(addr, StoragePath, key2, 1, true)
	states[1].versionMap.Delete(addr, BalancePath, accounts.NilKey, 1, true)

	// Tx3 read
	// we need to flush the local state objects as we're not
	// resetting the state - which is artificial for the test
	states[3].stateObjects = map[accounts.Address]*stateObject{}
	states[3].versionedReads = ReadSet{}
	v, err = states[3].GetState(addr, key1)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	v, err = states[3].GetState(addr, key2)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	b, err = states[3].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, b)

	// Tx1 delete
	for h := range states[1].versionedWrites.AllHeaders() {
		mvhm.Delete(h.Address, h.Path, h.Key, 1, true)
	}
	states[1].versionedWrites = WriteSet{}

	// Tx3 read
	states[3].versionedReads = ReadSet{}
	v, err = states[3].GetState(addr, key1)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	v, err = states[3].GetState(addr, key2)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, v)
	b, err = states[3].GetBalance(addr)
	assert.NoError(t, err)
	assert.Equal(t, uint256.Int{}, b)
}

// requireSameObservableState fails when the two states disagree on any value
// the test writes, so ApplyVersionedWrites is pinned against the single-process
// reference rather than only against "did not return an error".
func requireSameObservableState(t *testing.T, want, got *IntraBlockState, addrs []accounts.Address, keys []accounts.StorageKey) {
	t.Helper()
	for _, addr := range addrs {
		wantBal, err := want.GetBalance(addr)
		require.NoError(t, err)
		gotBal, err := got.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, wantBal, gotBal, "balance of %x", addr)

		wantNonce, err := want.GetNonce(addr)
		require.NoError(t, err)
		gotNonce, err := got.GetNonce(addr)
		require.NoError(t, err)
		require.Equal(t, wantNonce, gotNonce, "nonce of %x", addr)

		wantCode, err := want.GetCode(addr)
		require.NoError(t, err)
		gotCode, err := got.GetCode(addr)
		require.NoError(t, err)
		require.Equal(t, wantCode, gotCode, "code of %x", addr)

		wantDead, err := want.HasSelfdestructed(addr)
		require.NoError(t, err)
		gotDead, err := got.HasSelfdestructed(addr)
		require.NoError(t, err)
		require.Equal(t, wantDead, gotDead, "selfdestruct of %x", addr)

		for _, key := range keys {
			wantVal, err := want.GetState(addr, key)
			require.NoError(t, err)
			gotVal, err := got.GetState(addr, key)
			require.NoError(t, err)
			require.Equal(t, wantVal, gotVal, "storage %x of %x", key, addr)
		}
	}
}

func TestApplyVersionedWrites(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()

	sClean := New(reader)
	defer sClean.Close()
	sSingleProcess := New(reader)
	defer sSingleProcess.Close()

	states := []*IntraBlockState{s}

	// Create copies of the original state for each transition
	for i := 1; i <= 4; i++ {
		sCopy := NewWithVersionMap(reader, mvhm)
		defer sCopy.Close() //nolint:gocritic
		sCopy.txIndex = i
		states = append(states, sCopy)
	}

	addr1 := accounts.InternAddress(common.HexToAddress("0x01"))
	addr2 := accounts.InternAddress(common.HexToAddress("0x02"))
	addr3 := accounts.InternAddress(common.HexToAddress("0x03"))
	key1 := accounts.InternKey(common.HexToHash("0x01"))
	key2 := accounts.InternKey(common.HexToHash("0x02"))
	val1 := u256.U64(1)
	balance1 := uint256.NewInt(100)
	val2 := u256.U64(2)
	balance2 := uint256.NewInt(200)
	code := []byte{1, 2, 3}
	addrs := []accounts.Address{addr1, addr2, addr3}
	keys := []accounts.StorageKey{key1, key2}

	// Tx0 write
	_, err := states[0].GetOrNewStateObject(addr1)
	require.NoError(t, err)
	require.NoError(t, states[0].SetState(addr1, key1, val1))
	require.NoError(t, states[0].SetBalance(addr1, *balance1, tracing.BalanceChangeUnspecified))
	require.NoError(t, states[0].SetState(addr2, key2, val2))
	_, err = states[0].GetOrNewStateObject(addr3)
	require.NoError(t, err)
	require.NoError(t, states[0].FinalizeTx(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))
	states[0].versionMap.FlushVersionedWrites(states[0].VersionedWrites(), true)

	_, err = sSingleProcess.GetOrNewStateObject(addr1)
	require.NoError(t, err)
	require.NoError(t, sSingleProcess.SetState(addr1, key1, val1))
	require.NoError(t, sSingleProcess.SetBalance(addr1, *balance1, tracing.BalanceChangeUnspecified))
	require.NoError(t, sSingleProcess.SetState(addr2, key2, val2))
	_, err = sSingleProcess.GetOrNewStateObject(addr3)
	require.NoError(t, err)

	require.NoError(t, sClean.ApplyVersionedWrites(states[0].VersionedWrites()))
	requireSameObservableState(t, sSingleProcess, sClean, addrs, keys)

	// Tx1 write
	require.NoError(t, states[1].SetState(addr1, key2, val2))
	require.NoError(t, states[1].SetBalance(addr1, *balance2, tracing.BalanceChangeUnspecified))
	require.NoError(t, states[1].SetNonce(addr1, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, states[1].FinalizeTx(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))
	states[1].versionMap.FlushVersionedWrites(states[1].VersionedWrites(), true)

	require.NoError(t, sSingleProcess.SetState(addr1, key2, val2))
	require.NoError(t, sSingleProcess.SetBalance(addr1, *balance2, tracing.BalanceChangeUnspecified))
	require.NoError(t, sSingleProcess.SetNonce(addr1, 1, tracing.NonceChangeUnspecified))

	require.NoError(t, sClean.ApplyVersionedWrites(states[1].VersionedWrites()))
	requireSameObservableState(t, sSingleProcess, sClean, addrs, keys)

	// Tx2 write
	require.NoError(t, states[2].SetState(addr1, key1, val2))
	require.NoError(t, states[2].SetBalance(addr1, *balance2, tracing.BalanceChangeUnspecified))
	require.NoError(t, states[2].SetNonce(addr1, 2, tracing.NonceChangeUnspecified))
	require.NoError(t, states[2].FinalizeTx(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))
	states[2].versionMap.FlushVersionedWrites(states[2].VersionedWrites(), true)

	require.NoError(t, sSingleProcess.SetState(addr1, key1, val2))
	require.NoError(t, sSingleProcess.SetBalance(addr1, *balance2, tracing.BalanceChangeUnspecified))
	require.NoError(t, sSingleProcess.SetNonce(addr1, 2, tracing.NonceChangeUnspecified))

	require.NoError(t, sClean.ApplyVersionedWrites(states[2].VersionedWrites()))
	requireSameObservableState(t, sSingleProcess, sClean, addrs, keys)

	// Tx3 write
	// Materialize addr2 first: Selfdestruct on an object this state never read
	// silently no-ops, so the write set would carry no SelfDestructPath entry.
	_, err = states[3].GetOrNewStateObject(addr2)
	require.NoError(t, err)
	destructed, err := states[3].Selfdestruct(addr2, false)
	require.NoError(t, err)
	require.True(t, destructed)
	require.NoError(t, states[3].SetCode(addr1, code, tracing.CodeChangeUnspecified))
	require.NoError(t, states[3].FinalizeTx(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))
	states[3].versionMap.FlushVersionedWrites(states[3].VersionedWrites(), true)

	destructed, err = sSingleProcess.Selfdestruct(addr2, false)
	require.NoError(t, err)
	require.True(t, destructed)
	require.NoError(t, sSingleProcess.SetCode(addr1, code, tracing.CodeChangeUnspecified))

	require.NoError(t, sClean.ApplyVersionedWrites(states[3].VersionedWrites()))
	requireSameObservableState(t, sSingleProcess, sClean, addrs, keys)
}

// TestMakeWriteSetClearsCodeDomainOnEmptyOverride pins that clearing an
// account's code (e.g. an eth_simulateV1 stateOverride of "code":"0x") writes
// through to the CodeDomain, keeping it consistent with the now-empty account
// codeHash. Regression: gating the code write on a non-nil code slice skipped
// the clear, leaving the CodeDomain holding stale code and tripping the
// commitment codeHash-mismatch assert.
func TestMakeWriteSetClearsCodeDomainOnEmptyOverride(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)

	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	addrVal := addr.Value()
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}

	deploy := New(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer deploy.Close()
	require.NoError(t, deploy.CreateAccount(addr, true))
	require.NoError(t, deploy.SetNonce(addr, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, deploy.SetCode(addr, code, tracing.CodeChangeUnspecified))
	require.NoError(t, deploy.MakeWriteSet(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 0)))

	got, _, err := domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}).GetLatest(kv.CodeDomain, addrVal[:], kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, code, got)

	clear := New(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer clear.Close()
	require.NoError(t, clear.SetCode(addr, []byte{}, tracing.CodeChangeUnspecified))
	require.NoError(t, clear.MakeWriteSet(&chain.Rules{}, NewWriter(domains.AsPutDel(tx), nil, 1)))

	got, _, err = domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}).GetLatest(kv.CodeDomain, addrVal[:], kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Empty(t, got, "clearing code must clear the CodeDomain entry")
}

// erroringReader wraps NoopReader and fails ReadAccountData for one address,
// simulating a DB read failure.
type erroringReader struct {
	*NoopReader
	errAddr accounts.Address
	err     error
}

func (r *erroringReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	if address == r.errAddr {
		return nil, r.err
	}
	return r.NoopReader.ReadAccountData(address)
}

// TestPropagatesBalanceIncGetStateObjectError pins that a DB read failure while
// materializing a pending ripemd touch aborts the call instead of being silently
// dropped, which would skip the touch that state clearing and trie consistency
// depend on.
func TestPropagatesBalanceIncGetStateObjectError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		call func(*IntraBlockState) error
	}{
		{"FinalizeTx", func(sdb *IntraBlockState) error {
			return sdb.FinalizeTx(&chain.Rules{}, NewNoopWriter())
		}},
		{"CommitBlock", func(sdb *IntraBlockState) error {
			return sdb.CommitBlock(&chain.Rules{}, NewNoopWriter())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wantErr := errors.New("read failed")
			reader := &erroringReader{NoopReader: NewNoopReader(), errAddr: ripemd, err: wantErr}
			sdb := New(reader)
			defer sdb.Close()

			require.NoError(t, sdb.AddBalance(ripemd, uint256.Int{}, tracing.BalanceChangeUnspecified))

			require.ErrorIs(t, tc.call(sdb), wantErr)
		})
	}
}

// TestRevertSetCodeOnCodelessAccount pins the revert of a code change whose
// previous code was empty on an account that outlives the revert, e.g. an EOA.
func TestRevertSetCodeOnCodelessAccount(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))

	snapshot := ibs.PushSnapshot()
	require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
	ibs.RevertToSnapshot(snapshot, nil)

	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Empty(t, code)
	codeHash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, codeHash)
}

// TestSetCodeReusesTheLastEqualCode pins the SetCode memo: an equal code from a
// separate allocation gets the previous code's bytes and hash, a different code
// gets its own.
func TestSetCodeReusesTheLastEqualCode(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	codeA := []byte{0x60, 0x01, 0x60, 0x00, 0xf3}
	codeB := []byte{0x60, 0x02, 0x60, 0x00, 0xf3}
	var stored [][]byte
	for i, code := range [][]byte{codeA, bytes.Clone(codeA), codeB, bytes.Clone(codeA)} {
		addr := accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1))))
		require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeContractCreation))

		got, err := ibs.GetCode(addr)
		require.NoError(t, err)
		require.Equal(t, code, got)
		codeHash, err := ibs.GetCodeHash(addr)
		require.NoError(t, err)
		require.Equal(t, accounts.InternCodeHash(crypto.Keccak256Hash(code)), codeHash)
		stored = append(stored, got)
	}
	require.Same(t, &stored[0][0], &stored[1][0], "an equal code reuses the previous one")
	require.NotSame(t, &stored[0][0], &stored[3][0], "a different code in between replaces the memo")
}

// A pooled IntraBlockState serves call after call: nothing of one call may be
// visible to the next, while the read set keeps its maps.
func TestResetForPoolCarriesNothingToTheNextCall(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	ibs, vm := newNoMaterializeIBS(NewNoopReader())
	startNoMaterializeTx(ibs, vm, 0)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(7), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(9)))
	ibs.AddLog(&types.Log{Address: addr.Value()})
	ibs.AddAddressToAccessList(addr)
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true) // an address with no cell is not memoized
	ibs.readSelfDestructMemo(addr)
	require.NotEmpty(t, ibs.sdProbe)
	require.NotNil(t, ibs.versionedReads.address)
	ibs.SetTxContext(5, 2)
	ibs.SetVersion(3)
	ibs.eip8246, ibs.eip161, ibs.isAura = true, true, true

	require.True(t, ibs.resetForPool())
	require.Nil(t, ibs.stateReader)
	require.Zero(t, ibs.blockNum)
	require.Zero(t, ibs.version)
	require.False(t, ibs.eip8246 || ibs.eip161 || ibs.isAura, "fork flags are the next call's to set")
	require.Empty(t, ibs.sdProbe)
	require.NotNil(t, ibs.versionedReads.address, "the read set keeps its maps")
	require.Empty(t, ibs.versionedReads.address)

	ibs.stateReader = NewNoopReader()
	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.True(t, balance.IsZero())
	value, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.True(t, value.IsZero())
	require.Empty(t, ibs.GetRawLogs(0))
	require.False(t, ibs.AddressInAccessList(addr))
}

// The caller reads a call's output after its ibs goes back to the pool, so the
// next call must not write into that buffer.
func TestResetForPoolDropsTheOutputBuffer(t *testing.T) {
	ibs := New(NewNoopReader())
	ibs.txOutputFree = true
	out := ibs.TxOutputBuffer()
	*out = append((*out)[:0], 1, 2, 3)
	kept := *out

	require.True(t, ibs.resetForPool())
	ibs.txOutputFree = true
	next := ibs.TxOutputBuffer()
	*next = append((*next)[:0], 9, 9, 9)
	require.Equal(t, []byte{1, 2, 3}, kept)
}

// Maps never shrink, so a call that grew the state past the bound is not pooled.
func TestResetForPoolDropsAnOversizedState(t *testing.T) {
	ibs := New(NewNoopReader())
	for i := range maxPooledEntries + 1 {
		require.NoError(t, ibs.AddBalance(accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i+1)))), *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	}
	require.False(t, ibs.resetForPool())

	reads := New(NewNoopReader())
	for i := range maxPooledEntries + 1 {
		reads.versionedReads.SetCodeSize(accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i+1)))), VersionedRead[int]{})
	}
	require.False(t, reads.resetForPool(), "any read-set map counts toward the bound")

	warm := New(NewNoopReader())
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	for i := range maxPooledEntries + 1 {
		warm.AddSlotToAccessList(addr, accounts.InternKey(common.BigToHash(big.NewInt(int64(i)))))
	}
	require.False(t, warm.resetForPool(), "access-list slots count toward the bound")

	absent := New(NewNoopReader())
	for i := range maxPooledEntries + 1 {
		_, err := absent.GetBalance(accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1)))))
		require.NoError(t, err)
	}
	require.False(t, absent.resetForPool(), "absent-account memos count toward the bound")
}

// sync.Pool may drop any one entry, so the reuse shows over a few round trips.
func TestReleasePooledHandsTheStateToNewPooled(t *testing.T) {
	for range 100 {
		ibs := NewPooled(NewNoopReader())
		ReleasePooled(ibs)
		got := NewPooled(NewNoopReader())
		ReleasePooled(got)
		if got == ibs {
			return
		}
	}
	t.Fatal("NewPooled never got the released state back")
}

func TestPooledStateRoundTripIsLikeNew(t *testing.T) {
	ibs := NewPooled(NewNoopReader())
	ibs.SetTxContext(5, 2)
	ReleasePooled(ibs)

	// sync.Pool may drop the entry at any GC, so assert the release handed the
	// state on rather than that the next Get returns this one: Close would have
	// dropped the maps.
	require.NotNil(t, ibs.stateObjects, "a poolable state must be handed on, not closed")
	require.Nil(t, ibs.stateReader, "and must not keep the last call's reader")
	require.Zero(t, ibs.blockNum)
	require.Zero(t, ibs.txIndex)

	reader := NewNoopReader()
	got := NewPooled(reader)
	defer ReleasePooled(got)
	require.Same(t, reader, got.stateReader)
	require.Zero(t, got.txIndex)
}

// An oversized state must not reach the pool, so the next call does not inherit
// its map capacity.
func TestReleasePooledDropsAnOversizedState(t *testing.T) {
	big := NewPooled(NewNoopReader())
	for i := range maxPooledEntries + 1 {
		require.NoError(t, big.AddBalance(accounts.InternAddress(common.BigToAddress(big2(i+1))), *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	}
	ReleasePooled(big)

	fresh := NewPooled(NewNoopReader())
	defer ReleasePooled(fresh)
	require.NotSame(t, big, fresh, "an oversized state must not come back from the pool")
}

// A state closed before its release has lost its maps, so it must not reach the
// pool for the next call to write into.
func TestReleasePooledDropsAClosedState(t *testing.T) {
	closed := NewPooled(NewNoopReader())
	closed.Close()
	ReleasePooled(closed)

	fresh := NewPooled(NewNoopReader())
	defer ReleasePooled(fresh)
	require.NotSame(t, closed, fresh, "a closed state must not come back from the pool")
}

// A call that warms slots and then reverts keeps the grown slot maps, so it is
// not poolable even though nothing is live.
func TestResetForPoolDropsARevertedWarmUp(t *testing.T) {
	ibs := New(NewNoopReader())
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	snap := ibs.PushSnapshot()
	for i := range maxPooledEntries + 1 {
		ibs.AddSlotToAccessList(addr, accounts.InternKey(common.BigToHash(big2(i))))
	}
	ibs.RevertToSnapshot(snap, nil)
	live := len(ibs.accessList.addresses)
	for _, s := range ibs.accessList.slots {
		live += len(s)
	}
	require.Zero(t, live, "the revert leaves nothing live")
	require.False(t, ibs.resetForPool(), "the grown slot maps are still retained")
}

func big2(i int) *big.Int { return big.NewInt(int64(i)) }

// On a versioned IBS that caches state objects, an account touched and then
// credited in the same tx is not empty and must survive FinalizeTx.
func TestFinalizeTxKeepsTouchedThenCreditedAccount(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x161A"))
	rules := &chain.Rules{IsSpuriousDragon: true}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(newAccountStateReader(), vm)
	defer ibs.Close()

	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.TouchAccount(addr))
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
	vm.FlushVersionedWrites(ibs.FinalizedWrites(rules), true)
	ibs.ResetVersionedIO()

	ibs.SetTxContext(1, 1)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(5), bal)
}
