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
)

// postTxIBS simulates the IntraBlockState *after* a transaction where deletedAddr
// no longer exists (GetCodeHash returns NilCodeHash) and all other accounts are
// codeless-but-existent (EmptyCodeHash).
type postTxIBS struct {
	deletedAddr accounts.Address
}

func (m *postTxIBS) GetBalance(accounts.Address) (uint256.Int, error) { return uint256.Int{}, nil }
func (m *postTxIBS) GetNonce(accounts.Address) (uint64, error)        { return 0, nil }
func (m *postTxIBS) GetCode(accounts.Address) ([]byte, error)         { return nil, nil }
func (m *postTxIBS) GetState(accounts.Address, accounts.StorageKey) (uint256.Int, error) {
	return uint256.Int{}, nil
}
func (m *postTxIBS) Exist(accounts.Address) (bool, error) { return false, nil }
func (m *postTxIBS) GetRefund() uint64                    { return 0 }
func (m *postTxIBS) GetCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	if addr == m.deletedAddr {
		return accounts.NilCodeHash, nil
	}
	return accounts.EmptyCodeHash, nil
}

// TestPrestateTracerDiffModeDeletedAccount verifies that an account deleted during
// a tx appears in the diff-mode post state with codeHash == 0x000...000.
func TestPrestateTracerDiffModeDeletedAccount(t *testing.T) {
	deletedAddr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000001234"))

	tr := &prestateTracer{
		pre:     state{},
		post:    state{},
		config:  prestateTracerConfig{DiffMode: true, DisableCode: true, DisableStorage: true},
		created: make(map[accounts.Address]bool),
		deleted: make(map[accounts.Address]bool),
	}

	// Pre-tx: codeless account — no CodeHash set, balance = 0.
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

// TestPrestateTracerDiffModeCodelessUnchanged verifies that a codeless account
// with no state changes does NOT appear in the post state (no false positive).
func TestPrestateTracerDiffModeCodelessUnchanged(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000005678"))
	// Use a different deleted addr so that `addr` is treated as still-existent.
	otherAddr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000009999"))

	tr := &prestateTracer{
		pre:     state{},
		post:    state{},
		config:  prestateTracerConfig{DiffMode: true, DisableCode: true, DisableStorage: true},
		created: make(map[accounts.Address]bool),
		deleted: make(map[accounts.Address]bool),
	}

	tr.pre[addr] = &account{Balance: big.NewInt(0)}

	tr.env = &tracing.VMContext{
		IntraBlockState: &postTxIBS{deletedAddr: otherAddr}, // addr returns EmptyCodeHash
	}

	tr.processDiffState()

	_, ok := tr.post[addr]
	require.False(t, ok, "unchanged codeless account must NOT appear in post state")
}
