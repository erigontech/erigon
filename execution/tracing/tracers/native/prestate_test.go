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

package native

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

var _ tracing.IntraBlockState = (*postTxIBS)(nil)

// postTxIBS simulates the IntraBlockState *after* a transaction where deletedAddr
// no longer exists (GetCodeHash returns NilCodeHash) and all other accounts are
// codeless-but-existent (EmptyCodeHash).
type postTxIBS struct {
	deletedAddr accounts.Address
}

func (m *postTxIBS) GetBalance(accounts.Address) (uint256.Int, error) { return uint256.Int{}, nil }
func (m *postTxIBS) GetNonce(accounts.Address) (uint64, error)        { return 0, nil }
func (m *postTxIBS) GetCode(accounts.Address) ([]byte, error)         { return nil, nil }
func (m *postTxIBS) GetCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	if addr == m.deletedAddr {
		return accounts.NilCodeHash, nil
	}
	return accounts.EmptyCodeHash, nil
}
func (m *postTxIBS) GetState(accounts.Address, accounts.StorageKey) (uint256.Int, error) {
	return uint256.Int{}, nil
}
func (m *postTxIBS) Exist(accounts.Address) (bool, error) { return false, nil }
func (m *postTxIBS) GetRefund() uint64                    { return 0 }

func newTestPrestateTracer(cfg prestateTracerConfig) *prestateTracer {
	return &prestateTracer{
		pre:     state{},
		post:    state{},
		config:  cfg,
		created: make(map[accounts.Address]bool),
		deleted: make(map[accounts.Address]bool),
	}
}

// fakeOpContext is a minimal tracing.OpContext carrying only a stack and the
// executing contract address, as seen by OnOpcode.
type fakeOpContext struct {
	stack []uint256.Int
	addr  accounts.Address
}

func (c *fakeOpContext) MemoryData() []byte          { return nil }
func (c *fakeOpContext) StackData() []uint256.Int    { return c.stack }
func (c *fakeOpContext) Caller() accounts.Address    { return c.addr }
func (c *fakeOpContext) Address() accounts.Address   { return c.addr }
func (c *fakeOpContext) CallValue() uint256.Int      { return uint256.Int{} }
func (c *fakeOpContext) CallInput() []byte           { return nil }
func (c *fakeOpContext) Code() []byte                { return nil }
func (c *fakeOpContext) CodeHash() accounts.CodeHash { return accounts.EmptyCodeHash }

// TestPrestateTracerOnOpcodeFaultedSkipsLookup verifies that an opcode invoked
// with a non-nil err (fault path, e.g. out-of-gas at the opcode itself) does not
// record any touched account or storage slot: in consensus terms the access never
// happened (no EIP-2929 warm-up), and go-ethereum skips it since PR #26848.
func TestPrestateTracerOnOpcodeFaultedSkipsLookup(t *testing.T) {
	caller := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000001111"))
	target := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000002222"))

	tr := newTestPrestateTracer(prestateTracerConfig{})
	tr.env = &tracing.VMContext{
		IntraBlockState: &postTxIBS{},
	}
	tr.lookupAccount(caller)

	// EXTCODESIZE faulting with the target address as operand
	// (real-world case: mainnet tx 0x84357b59..., block 25634962, OOG at EXTCODESIZE).
	targetAddr := target.Value()
	var operand uint256.Int
	operand.SetBytes(targetAddr[:])
	stack := []uint256.Int{operand}
	tr.OnOpcode(0, byte(vm.EXTCODESIZE), 1724, 2600, &fakeOpContext{stack: stack, addr: caller}, nil, 2, vm.ErrOutOfGas)

	_, ok := tr.pre[target]
	require.False(t, ok, "account referenced by a faulted EXTCODESIZE must not be recorded in the prestate")

	// SLOAD faulting with the slot key as operand
	// (real-world case: mainnet tx 0xa4b924b4..., block 25638021, OOG at SLOAD).
	slot := common.HexToHash("0xbaaed5f3d2bc4b0bc4f1758fde25c1522c4254f5b2fbfa513449670cff246a98")
	operand.SetBytes(slot[:])
	stack = []uint256.Int{operand}
	tr.OnOpcode(0, byte(vm.SLOAD), 1577, 2100, &fakeOpContext{stack: stack, addr: caller}, nil, 1, vm.ErrOutOfGas)

	require.NotContains(t, tr.pre[caller].Storage, slot,
		"storage slot referenced by a faulted SLOAD must not be recorded in the prestate")
}

// TestPrestateTracerDiffModeDeletedAccount verifies that an account deleted during
// a tx appears in the diff-mode post state with codeHash == 0x000...000.
func TestPrestateTracerDiffModeDeletedAccount(t *testing.T) {
	deletedAddr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000001234"))

	tr := newTestPrestateTracer(prestateTracerConfig{DiffMode: true, DisableCode: true, DisableStorage: true})

	tr.pre[deletedAddr] = &account{Balance: (*hexutil.Big)(big.NewInt(0))}

	tr.env = &tracing.VMContext{
		IntraBlockState: &postTxIBS{deletedAddr: deletedAddr},
	}

	tr.processDiffState()

	post, ok := tr.post[deletedAddr]
	require.True(t, ok, "deleted account must appear in post state")
	require.NotNil(t, post.CodeHash, "deleted account must carry codeHash in post state")
	require.Equal(t, common.Hash{}, *post.CodeHash, "deleted account must have zero codeHash")
}

// TestPrestateTracerOnTxEndExcludesAccountEmptyBeforeStorageTouched pins the
// account.empty invariant (see its field doc) against a virgin-SLOAD account.
func TestPrestateTracerOnTxEndExcludesAccountEmptyBeforeStorageTouched(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000004242"))
	otherAddr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000009999"))

	tr := newTestPrestateTracer(prestateTracerConfig{})
	tr.env = &tracing.VMContext{
		IntraBlockState: &postTxIBS{deletedAddr: otherAddr},
	}

	tr.lookupAccount(addr)
	tr.lookupStorage(addr, common.HexToHash("0x01"))

	tr.OnTxEnd(nil, nil)

	_, ok := tr.pre[addr]
	require.False(t, ok, "account empty before the tx must be excluded even though its storage was read during the tx")
}

// TestPrestateTracerDiffModeCodelessUnchanged verifies that a codeless account
// with no state changes does NOT appear in the post state (no false positive).
func TestPrestateTracerDiffModeCodelessUnchanged(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000005678"))
	// Use a different deleted addr so that `addr` is treated as still-existent.
	otherAddr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000009999"))

	tr := newTestPrestateTracer(prestateTracerConfig{DiffMode: true, DisableCode: true, DisableStorage: true})

	tr.pre[addr] = &account{Balance: (*hexutil.Big)(big.NewInt(0))}

	tr.env = &tracing.VMContext{
		IntraBlockState: &postTxIBS{deletedAddr: otherAddr},
	}

	tr.processDiffState()

	_, ok := tr.post[addr]
	require.False(t, ok, "unchanged codeless account must NOT appear in post state")
}

// TestPrestateTracerDiffModeZeroStorageUnmodified verifies that a storage slot
// read as zero and unchanged (e.g. an SLOAD on a virgin slot) does not create
// a spurious diff entry: the account must be excluded from both pre and post.
func TestPrestateTracerDiffModeZeroStorageUnmodified(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000004242"))
	otherAddr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000009999"))

	tr := newTestPrestateTracer(prestateTracerConfig{DiffMode: true})

	tr.pre[addr] = &account{
		Balance: (*hexutil.Big)(big.NewInt(0)),
		Storage: map[common.Hash]common.Hash{
			common.HexToHash("0x01"): {},
		},
	}

	tr.env = &tracing.VMContext{
		IntraBlockState: &postTxIBS{deletedAddr: otherAddr},
	}

	tr.processDiffState()

	_, inPre := tr.pre[addr]
	require.False(t, inPre, "unmodified account with only a zero storage slot must not remain in pre state")
	_, inPost := tr.post[addr]
	require.False(t, inPost, "unmodified account with only a zero storage slot must not appear in post state")
}

// A MarshalJSON on callFrame makes encoding/json re-scan every nested frame's
// bytes at each level, which is quadratic in call depth. Reflection over the
// wire types encodes children inline instead.
func TestCallFrameHasNoJSONMarshaler(t *testing.T) {
	t.Parallel()
	_, bad := any(&callFrame{}).(json.Marshaler)
	require.False(t, bad, "callFrame must not implement json.Marshaler")
}

// The flat frames are encoded as a slice, so a MarshalJSON on any of them sends
// the whole slice down marshalerEncoder and re-scans each frame's bytes. The
// output tests below pass either way, so the absence is asserted here.
func TestFlatCallTypesHaveNoJSONMarshaler(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"flatCallFrame", &flatCallFrame{}},
		{"flatCallAction", &flatCallAction{}},
		{"flatCallResult", &flatCallResult{}},
	} {
		_, bad := tc.v.(json.Marshaler)
		require.False(t, bad, "%s must not implement json.Marshaler", tc.name)
	}
}

// A zero value and a call to the zero address are both real, so only a nil
// pointer may be omitted. The flat tracer depends on this: it fills in a zero
// value for child calls precisely so the key is present.
func TestOnlyNilPointersAreOmitted(t *testing.T) {
	t.Parallel()
	zeroV := hexutil.U256(*uint256.NewInt(0))
	var zeroAddr common.Address

	var set callFrame
	set.setType(vm.CALL)
	set.Value, set.To = &zeroV, &zeroAddr
	b, err := json.Marshal(set)
	require.NoError(t, err)
	require.Contains(t, string(b), `"value":"0x0"`)
	require.Contains(t, string(b), `"to":"0x0000000000000000000000000000000000000000"`)

	var unset callFrame
	unset.setType(vm.CREATE)
	b, err = json.Marshal(unset)
	require.NoError(t, err)
	require.NotContains(t, string(b), `"value"`)
	require.NotContains(t, string(b), `"to"`)
	require.Contains(t, string(b), `"input":"0x"`, "input carries no omitempty")
}

// The flat tracer's output is the public parity/OE trace shape. These cases pin
// it exactly — field names, ordering, omissions and quantity encoding — because
// the wire types on the frame are all that produce it now.
func TestFlatCallFrameJSON(t *testing.T) {
	t.Parallel()

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	zero := hexutil.U256(*uint256.NewInt(0))
	maxU256 := hexutil.U256(*new(uint256.Int).SetAllOne())

	for _, tc := range []struct {
		name  string
		build func() *flatCallFrame
		want  string
	}{
		{
			name: "call",
			build: func() *flatCallFrame {
				f := &callFrame{From: from, To: &to, Gas: 0x1234, GasUsed: 0x100,
					Input: hexutil.Bytes{0xaa, 0xbb}, Output: hexutil.Bytes{0xcc}, Value: &zero}
				f.setType(vm.CALL)
				return newFlatCall(f)
			},
			want: `{"action":{"callType":"call","from":"0x1111111111111111111111111111111111111111","gas":"0x1234","input":"0xaabb","to":"0x2222222222222222222222222222222222222222","value":"0x0"},"blockHash":null,"blockNumber":0,"result":{"gasUsed":"0x100","output":"0xcc"},"subtraces":0,"traceAddress":null,"transactionHash":null,"transactionPosition":0,"type":"call"}`,
		},
		{
			name: "delegatecall keeps its callType and a max value",
			build: func() *flatCallFrame {
				f := &callFrame{From: from, To: &to, Gas: 1, GasUsed: 2, Value: &maxU256}
				f.setType(vm.DELEGATECALL)
				return newFlatCall(f)
			},
			want: `{"action":{"callType":"delegatecall","from":"0x1111111111111111111111111111111111111111","gas":"0x1","input":"0x","to":"0x2222222222222222222222222222222222222222","value":"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},"blockHash":null,"blockNumber":0,"result":{"gasUsed":"0x2","output":"0x"},"subtraces":0,"traceAddress":null,"transactionHash":null,"transactionPosition":0,"type":"call"}`,
		},
		{
			name: "create2 reports the deployed code and address",
			build: func() *flatCallFrame {
				f := &callFrame{From: from, To: &to, Gas: 0x10, GasUsed: 0x8,
					Input: hexutil.Bytes{0x60, 0x80}, Output: hexutil.Bytes{0xfe}, Value: &zero}
				f.setType(vm.CREATE2)
				return newFlatCreate(f)
			},
			want: `{"action":{"creationMethod":"create2","from":"0x1111111111111111111111111111111111111111","gas":"0x10","init":"0x6080","value":"0x0"},"blockHash":null,"blockNumber":0,"result":{"address":"0x2222222222222222222222222222222222222222","code":"0xfe","gasUsed":"0x8"},"subtraces":0,"traceAddress":null,"transactionHash":null,"transactionPosition":0,"type":"create"}`,
		},
		{
			name: "selfdestruct carries no result",
			build: func() *flatCallFrame {
				f := &callFrame{From: from, To: &to, Value: &zero}
				f.setType(vm.SELFDESTRUCT)
				return newFlatSelfdestruct(f)
			},
			want: `{"action":{"address":"0x1111111111111111111111111111111111111111","balance":"0x0","refundAddress":"0x2222222222222222222222222222222222222222"},"blockHash":null,"blockNumber":0,"subtraces":0,"traceAddress":null,"transactionHash":null,"transactionPosition":0,"type":"suicide"}`,
		},
		{
			name: "nil and empty byte fields both encode as 0x",
			build: func() *flatCallFrame {
				f := &callFrame{From: from, To: &to, Input: nil, Output: hexutil.Bytes{}, Value: &zero}
				f.setType(vm.CALL)
				return newFlatCall(f)
			},
			want: `{"action":{"callType":"call","from":"0x1111111111111111111111111111111111111111","gas":"0x0","input":"0x","to":"0x2222222222222222222222222222222222222222","value":"0x0"},"blockHash":null,"blockNumber":0,"result":{"gasUsed":"0x0","output":"0x"},"subtraces":0,"traceAddress":null,"transactionHash":null,"transactionPosition":0,"type":"call"}`,
		},
		{
			name: "a failed call reports error and drops the result",
			build: func() *flatCallFrame {
				f := &callFrame{From: from, To: &to, Value: &zero}
				f.setType(vm.CALL)
				fc := newFlatCall(f)
				fc.Error, fc.Result = "Reverted", nil
				return fc
			},
			want: `{"action":{"callType":"call","from":"0x1111111111111111111111111111111111111111","gas":"0x0","input":"0x","to":"0x2222222222222222222222222222222222222222","value":"0x0"},"blockHash":null,"blockNumber":0,"error":"Reverted","subtraces":0,"traceAddress":null,"transactionHash":null,"transactionPosition":0,"type":"call"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tc.build())
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
}

// A MarshalJSON on account makes encoding/json re-parse every account's bytes
// while encoding the map that holds them. Reflection over the wire types does
// not.
func TestAccountHasNoJSONMarshaler(t *testing.T) {
	t.Parallel()
	_, bad := any(&account{}).(json.Marshaler)
	require.False(t, bad, "account must not implement json.Marshaler")
}
