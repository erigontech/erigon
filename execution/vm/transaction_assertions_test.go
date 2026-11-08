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

package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestTxDiffAccountReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		param uint64
	}{
		{"balance before", 2},
		{"balance after", 3},
		{"code hash before", 4},
		{"code hash after", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address := common.HexToAddress("0x2000")
			readErr := errors.New("account read failed")
			ibs := state.New(beneficiaryErrReader{
				StateReader: state.NewNoopReader(), fail: accounts.InternAddress(address), err: readErr,
			})
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
			evm.FrameContext = ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Mode: types.FramePostTx}}}, uint256.Int{})
			call := &CallContext{}
			call.Stack.push(uint256.Int{})
			call.Stack.push(*new(uint256.Int).SetBytes(address[:]))
			call.Stack.push(*uint256.NewInt(tc.param))

			_, _, err := opTxDiff(0, evm, call)
			require.ErrorIs(t, err, readErr)
			require.ErrorIs(t, err, ErrIntraBlockStateFailed)
		})
	}
}
