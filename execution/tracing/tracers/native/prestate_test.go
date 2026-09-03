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
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

	tr.pre[deletedAddr] = &account{Balance: big.NewInt(0)}

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

	tr.pre[addr] = &account{Balance: big.NewInt(0)}

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
		Balance: big.NewInt(0),
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

type discardWriter struct{ n int }

func (d *discardWriter) WriteRawBytes(p []byte) { d.n += len(p) }

// chunkRecorder keeps the bytes and the largest handover.
type chunkRecorder struct {
	out      []byte
	maxChunk int
}

func (c *chunkRecorder) WriteRawBytes(p []byte) {
	c.out = append(c.out, p...)
	c.maxChunk = max(c.maxChunk, len(p))
}

func encodeFrame(f *callFrame) string {
	var w byteWriter
	f.AppendJSON(&w)
	return string(w.b)
}

func mkBenchFrame(depth, width int) callFrame {
	to := common.HexToAddress("0x1111111111111111111111111111111111111111")
	f := callFrame{
		Type: vm.CALL, From: common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
		Gas: 21000, GasUsed: 12345, To: &to,
		Input: make([]byte, 128), Output: make([]byte, 64), Value: uint256.NewInt(1e18),
		Logs: []callLog{{Index: 1, Address: common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Topics: []common.Hash{common.HexToHash("0xaa"), {}}, Data: make([]byte, 96), Position: 0}},
	}
	if depth > 0 {
		for range width {
			f.Calls = append(f.Calls, mkBenchFrame(depth-1, width))
		}
	}
	return f
}

func TestAppendJSONMatchesGencodec(t *testing.T) {
	t.Parallel()
	for _, f := range []callFrame{
		mkBenchFrame(6, 2), // deep enough to hand the buffer over many times
		{Type: vm.CREATE, Error: `bad <thing> "x"`, Revertal: "a\tb"},
		{Type: vm.STATICCALL, Logs: []callLog{{Topics: nil}}},
	} {
		want, err := json.Marshal(f)
		require.NoError(t, err)

		var rec chunkRecorder
		f.AppendJSON(&rec)
		require.Equal(t, string(want), string(rec.out))
		// A chunk is handed over at frame boundaries, so it holds at most one
		// frame beyond the buffer. Frames here are uniform and small.
		require.LessOrEqual(t, rec.maxChunk, 2*chunkSize, "a chunk grew past the bound")
	}
}

func BenchmarkCallFrameMarshal(b *testing.B) {
	for _, depth := range []int{1, 3, 5, 7} {
		f := mkBenchFrame(depth, 2)
		b.Run(fmt.Sprintf("depth=%d/gencodec", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(f); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("depth=%d/appendJSON", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var w byteWriter
				f.AppendJSON(&w)
			}
		})
		b.Run(fmt.Sprintf("depth=%d/appendJSONToStream", depth), func(b *testing.B) {
			b.ReportAllocs()
			var w discardWriter
			for b.Loop() {
				f.AppendJSON(&w)
			}
		})
	}
}

// fillNonZero sets every field of v to a non-zero value, recursing into structs
// and allocating one element for slices and pointers. A field added to callFrame
// is therefore populated automatically, so TestAppendJSONCoversEveryField fails
// if appendJSON forgets it.
func fillNonZero(v reflect.Value, depth int) {
	switch v.Kind() {
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fillNonZero(v.Field(i), depth)
			}
		}
	case reflect.Slice:
		if depth <= 0 { // callFrame.Calls recurses; stop before it blows the stack
			return
		}
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillNonZero(v.Index(0), depth-1)
	case reflect.Pointer:
		if depth <= 0 {
			return
		}
		v.Set(reflect.New(v.Type().Elem()))
		fillNonZero(v.Elem(), depth-1)
	case reflect.Array:
		for i := range v.Len() {
			fillNonZero(v.Index(i), depth)
		}
	case reflect.String:
		v.SetString("x")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(7)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Bool:
		v.SetBool(true)
	}
}

func TestAppendJSONCoversEveryField(t *testing.T) {
	t.Parallel()
	var f callFrame
	fillNonZero(reflect.ValueOf(&f).Elem(), 2)
	f.Type = vm.CALL // fillNonZero picks an opcode that has no name

	want, err := json.Marshal(f)
	require.NoError(t, err)
	require.Equal(t, string(want), encodeFrame(&f),
		"AppendJSON drifted from the generated MarshalJSON - a new callFrame field is probably unhandled")
}
