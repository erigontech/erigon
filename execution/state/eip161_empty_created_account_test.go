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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// emptinessScenario drives an account through a lifecycle and reports the
// EIP-161 verdict. Each scenario runs on both the versioned and the serial
// path, which must agree.
type emptinessScenario struct {
	name  string
	steps func(t *testing.T, ibs *IntraBlockState, addr accounts.Address)
	want  bool
}

// emptinessMode is one of the three state configurations that must agree:
// the serial path, and the versioned path with the stateObject cache on and
// off. noMaterialize is the configuration staged sync runs.
type emptinessMode struct {
	name          string
	versioned     bool
	noMaterialize bool
}

var emptinessModes = []emptinessMode{
	{"serial", false, false},
	{"versioned", true, false},
	{"versioned+noMaterialize", true, true},
}

func runEmptiness(t *testing.T, mode emptinessMode, sc emptinessScenario, addr accounts.Address) bool {
	t.Helper()

	reader := NewNoopReader()
	var ibs *IntraBlockState
	if mode.versioned {
		ibs = NewWithVersionMap(reader, NewVersionMap(nil))
		ibs.SetNoMaterialize(mode.noMaterialize)
	} else {
		ibs = New(reader)
	}
	defer ibs.Close()
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	sc.steps(t, ibs, addr)

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	return empty
}

// TestEmptyAccountLifecycle pins the EIP-161 emptiness predicate
// across the account lifecycle on the versioned (parallel) path.
//
// EIP-161 defines empty purely by fields — no code, zero nonce, zero balance —
// so materializing an account must not change the verdict. createObject records
// SelfDestructPath=false for every account it materializes, and reading that
// write as evidence of a self-destruct made every freshly created empty account
// report non-empty for the rest of the transaction. That suppresses the
// EIP-8037 account-creation state gas on a later value-bearing CALL to the
// same address, so the two clients disagree on gas and diverge (issue #23670).
//
// The self-destruct cases are the control: they are what the gate exists for
// under EIP-6780 and must keep reporting non-empty.
func TestEmptyAccountLifecycle(t *testing.T) {
	t.Parallel()

	code := []byte{0x60, 0x00}

	scenarios := []emptinessScenario{
		{
			name:  "untouched absent account is empty",
			steps: func(*testing.T, *IntraBlockState, accounts.Address) {},
			want:  true,
		},
		{
			name: "materialized account with empty fields is empty",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, false))
			},
			want: true,
		},
		{
			name: "materialized account holding balance is not empty",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, false))
				require.NoError(t, ibs.SetBalance(addr, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
			},
			want: false,
		},
		{
			name: "contract self-destructed in this tx is not empty",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
				_, err := ibs.Selfdestruct(addr, false)
				require.NoError(t, err)
			},
			want: false,
		},
		{
			name: "self-destruct then recreate falls back to the recreated fields",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
				_, err := ibs.Selfdestruct(addr, false)
				require.NoError(t, err)
				require.NoError(t, ibs.CreateAccount(addr, false))
			},
			want: true,
		},
		{
			name: "recreate then self-destruct again is not empty",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
				_, err := ibs.Selfdestruct(addr, false)
				require.NoError(t, err)
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
				_, err = ibs.Selfdestruct(addr, false)
				require.NoError(t, err)
			},
			want: false,
		},
		{
			name: "reverted recreate leaves the self-destruct standing",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
				_, err := ibs.Selfdestruct(addr, false)
				require.NoError(t, err)
				snap := ibs.PushSnapshot()
				require.NoError(t, ibs.CreateAccount(addr, true))
				require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
				ibs.RevertToSnapshot(snap, nil)
			},
			want: false,
		},
		{
			name: "reverted self-destruct leaves the account judged by its fields",
			steps: func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
				require.NoError(t, ibs.CreateAccount(addr, false))
				snap := ibs.PushSnapshot()
				_, err := ibs.Selfdestruct(addr, false)
				require.NoError(t, err)
				ibs.RevertToSnapshot(snap, nil)
			},
			want: true,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			for _, mode := range emptinessModes {
				addr := toAddr([]byte(sc.name + mode.name))
				require.Equal(t, sc.want, runEmptiness(t, mode, sc, addr), mode.name)
			}
		})
	}
}
