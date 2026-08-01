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
	"math/big"
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
	stack := []uint256.Int{*new(uint256.Int).SetBytes(targetAddr[:])}
	tr.OnOpcode(0, byte(vm.EXTCODESIZE), 1724, 2600, &fakeOpContext{stack: stack, addr: caller}, nil, 2, vm.ErrOutOfGas)

	_, ok := tr.pre[target]
	require.False(t, ok, "account referenced by a faulted EXTCODESIZE must not be recorded in the prestate")

	// SLOAD faulting with the slot key as operand
	// (real-world case: mainnet tx 0xa4b924b4..., block 25638021, OOG at SLOAD).
	slot := common.HexToHash("0xbaaed5f3d2bc4b0bc4f1758fde25c1522c4254f5b2fbfa513449670cff246a98")
	stack = []uint256.Int{*new(uint256.Int).SetBytes(slot[:])}
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
