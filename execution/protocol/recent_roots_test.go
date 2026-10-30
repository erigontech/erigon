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

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func recentRootTestEnv(t *testing.T) (*vm.EVM, *types.FrameTransaction) {
	t.Helper()
	evm, tx := frameTestEnv(t)
	evm.Context.SlotNumber = 2
	address := common.HexToAddress("0x8272D9679689Ea2f307140CdF9002D27dC00Ffff")
	key := common.HexToHash("0x5f027aa1cbe2df279bf6518edd4b44ea5409fd800189ec35224e10ab05e574c3")
	entry := common.HexToHash("0x0a0d1254c851be5a133b4c9a9e300f5602fc0f43dbe65aa6a66930d4ca0a51b8")
	data := common.FromHex("b9382d35273c75a50631a3e84d3c75ec9266e2b18c35a627e16cdbf26a18ca8500000000000000010000000000000000000000000000000000000000000000000000000000000002")
	ibs := evm.IntraBlockState()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(address), RecentRootCode, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetState(accounts.InternAddress(address), accounts.InternKey(key), *new(uint256.Int).SetBytes(entry[:])))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Target: &address, Data: data, Limits: mdgas.MdGas{Execution: 20_000}},
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
	}
	return evm, tx
}

func TestFrameValidationRecentRoot(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(fmt.Sprintf("expiry=%t", expiry), func(t *testing.T) {
			evm, tx := recentRootTestEnv(t)
			var deadline uint64
			if expiry {
				deadline = evm.Context.Time + 1
				require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(types.FrameExpiryVerifier), frameExpiryCode, tracing.CodeChangeUnspecified))
				tx.Frames = append([]types.Frame{{
					Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier,
					Data: binary.BigEndian.AppendUint64(nil, deadline), Limits: mdgas.MdGas{Execution: 5_000},
				}}, tx.Frames...)
			}
			validation, err := ValidateFrameTransaction(evm, tx)
			require.NoError(t, err)
			require.Equal(t, deadline, validation.Deadline)
			require.Contains(t, validation.Dependencies, types.FrameRecentRootVerifier)
			require.Equal(t, []RecentRootReference{{
				StorageKey: common.HexToHash("0x5f027aa1cbe2df279bf6518edd4b44ea5409fd800189ec35224e10ab05e574c3"),
				EntryHash:  common.HexToHash("0x0a0d1254c851be5a133b4c9a9e300f5602fc0f43dbe65aa6a66930d4ca0a51b8"),
				Slot:       1,
			}}, validation.RecentRoots)
			refs, err := FrameTransactionRecentRoots(tx)
			require.NoError(t, err)
			require.Equal(t, validation.RecentRoots, refs)
		})
	}
}

func TestFrameValidationRecentRootPrefixes(t *testing.T) {
	for _, deploy := range []bool{false, true} {
		for _, sponsored := range []bool{false, true} {
			for _, expiry := range []bool{false, true} {
				for _, roots := range []bool{false, true} {
					t.Run(fmt.Sprintf("deploy=%t/sponsored=%t/expiry=%t/roots=%t", deploy, sponsored, expiry, roots), func(t *testing.T) {
						_, tx := recentRootTestEnv(t)
						root, sender := tx.Frames[0], tx.Frames[1]
						rootIndex, deployIndex := -1, -1
						tx.Frames = nil
						if expiry {
							tx.Frames = append(tx.Frames, types.Frame{Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: make([]byte, 8)})
						}
						if roots {
							rootIndex = len(tx.Frames)
							tx.Frames = append(tx.Frames, root)
						}
						if deploy {
							deployIndex = len(tx.Frames)
							tx.Frames = append(tx.Frames, types.Frame{Mode: types.FrameDefault})
						}
						if sponsored {
							sender.Flags = types.FrameApproveExecution
						}
						tx.Frames = append(tx.Frames, sender)
						if sponsored {
							payer := common.Address{9}
							tx.Frames = append(tx.Frames, types.Frame{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &payer})
						}
						prefix, err := frameValidationPrefix(tx)
						require.NoError(t, err)
						require.Equal(t, rootIndex, prefix.recentRoot)
						require.Equal(t, deployIndex, prefix.deploy)
						require.Equal(t, len(tx.Frames)-1, prefix.end)
					})
				}
			}
		}
	}
}

func TestFrameValidationRecentRootInvalidPrefix(t *testing.T) {
	for name, change := range map[string]func(*types.FrameTransaction){
		"empty data":      func(tx *types.FrameTransaction) { tx.Frames[0].Data = nil },
		"short tuple":     func(tx *types.FrameTransaction) { tx.Frames[0].Data = tx.Frames[0].Data[:71] },
		"long tuple":      func(tx *types.FrameTransaction) { tx.Frames[0].Data = append(tx.Frames[0].Data, 0) },
		"write calldata":  func(tx *types.FrameTransaction) { tx.Frames[0].Data = make([]byte, 64) },
		"too many tuples": func(tx *types.FrameTransaction) { tx.Frames[0].Data = bytes.Repeat(tx.Frames[0].Data, 17) },
		"flags":           func(tx *types.FrameTransaction) { tx.Frames[0].Flags = types.FrameApprovePayment },
		"state gas":       func(tx *types.FrameTransaction) { tx.Frames[0].Limits.State = 1 },
		"value":           func(tx *types.FrameTransaction) { tx.Frames[0].Value.SetOne() },
		"duplicate":       func(tx *types.FrameTransaction) { tx.Frames = append([]types.Frame{tx.Frames[0]}, tx.Frames...) },
		"after deployment": func(tx *types.FrameTransaction) {
			tx.Frames = append([]types.Frame{{Mode: types.FrameDefault}}, tx.Frames...)
		},
		"after approval": func(tx *types.FrameTransaction) { tx.Frames[0], tx.Frames[1] = tx.Frames[1], tx.Frames[0] },
		"before expiry": func(tx *types.FrameTransaction) {
			tx.Frames = []types.Frame{tx.Frames[0], {Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: make([]byte, 8)}, tx.Frames[1]}
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, tx := recentRootTestEnv(t)
			change(tx)
			require.Error(t, ValidateFrameTransactionPrefix(tx))
		})
	}
}

func TestFrameValidationRecentRootAge(t *testing.T) {
	for _, tc := range []struct {
		name  string
		slot  uint64
		valid bool
	}{
		{"previous slot", 2, true},
		{"current slot", 1, false},
		{"future slot", 0, false},
		{"oldest usable", 8192, true},
		{"expired", 8193, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, tx := recentRootTestEnv(t)
			evm.Context.SlotNumber = tc.slot
			_, err := ValidateFrameTransaction(evm, tx)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, vm.ErrExecutionReverted)
			}
		})
	}
	ref := RecentRootReference{Slot: math.MaxUint64 - 1}
	require.True(t, ref.ValidAt(math.MaxUint64))
	require.False(t, ref.ValidAt(0))
}

func TestFrameRecentRootPayloadSlot(t *testing.T) {
	evm, tx := recentRootTestEnv(t)
	evm.Context.SlotNumber = 8192
	_, err := ValidateFrameTransaction(evm, tx)
	require.NoError(t, err)

	// Empty slots can expire a root between pool validation and payload execution.
	evm.Context.SlotNumber = 8193
	_, err = applyFrameTest(t, evm, tx)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
	nonce, err := evm.IntraBlockState().GetNonce(accounts.InternAddress(tx.SenderAddress))
	require.NoError(t, err)
	require.Zero(t, nonce)
}

func TestFrameValidationRecentRootCodeAndStorage(t *testing.T) {
	for name, code := range map[string][]byte{
		"missing":           nil,
		"stop":              {byte(vm.STOP)},
		"different runtime": append(bytes.Clone(RecentRootCode), byte(vm.STOP)),
	} {
		t.Run(name, func(t *testing.T) {
			evm, tx := recentRootTestEnv(t)
			require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(types.FrameRecentRootVerifier), code, tracing.CodeChangeUnspecified))
			_, err := ValidateFrameTransaction(evm, tx)
			require.ErrorContains(t, err, "non-canonical recent root verifier code")
		})
	}
	t.Run("entry removed", func(t *testing.T) {
		evm, tx := recentRootTestEnv(t)
		refs, err := ParseRecentRootReferences(tx.Frames[0].Data)
		require.NoError(t, err)
		require.NoError(t, evm.IntraBlockState().SetState(accounts.InternAddress(types.FrameRecentRootVerifier), accounts.InternKey(refs[0].StorageKey), uint256.Int{}))
		_, err = ValidateFrameTransaction(evm, tx)
		require.ErrorIs(t, err, vm.ErrExecutionReverted)
	})
}

func TestFrameValidationRecentRootNestedCall(t *testing.T) {
	evm, tx := recentRootTestEnv(t)
	code := program.New().Mstore(tx.Frames[0].Data, 0).
		StaticCall(uint256.NewInt(20_000), types.FrameRecentRootVerifier, 0, len(tx.Frames[0].Data), 0, 0).
		Op(vm.POP).Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
	tx.Frames[1].Limits.Execution = 30_000
	_, err := ValidateFrameTransaction(evm, tx)
	require.ErrorContains(t, err, "SLOTNUM")
}

func TestFrameValidationRecentRootGas(t *testing.T) {
	evm, tx := recentRootTestEnv(t)
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	used := result.FrameReceipts[0].GasUsed.Execution
	for _, sufficient := range []bool{false, true} {
		t.Run(fmt.Sprint(sufficient), func(t *testing.T) {
			evm, tx := recentRootTestEnv(t)
			tx.Frames[0].Limits.Execution = used
			if !sufficient {
				tx.Frames[0].Limits.Execution--
			}
			_, err := ValidateFrameTransaction(evm, tx)
			if sufficient {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, vm.ErrOutOfGas)
			}
		})
	}
	_, tx = recentRootTestEnv(t)
	tx.Signatures = []types.FrameSignature{{Scheme: types.FrameSignatureArbitrary}}
	tx.Frames[0].Limits.Execution = 100_000 - tx.Frames[1].Limits.Execution - tx.Signatures[0].VerificationGas()
	require.NoError(t, ValidateFrameTransactionPrefix(tx))
	tx.Frames[0].Limits.Execution++
	require.ErrorContains(t, ValidateFrameTransactionPrefix(tx), "exceeds gas limit")
}

func TestFrameValidationRecentRootMultipleTuples(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
			evm, tx := recentRootTestEnv(t)
			tuple := tx.Frames[0].Data
			tx.Frames[0].Data = nil
			for i := range 16 {
				data := bytes.Clone(tuple)
				if !duplicate {
					binary.BigEndian.PutUint64(data[:8], uint64(i))
				}
				tx.Frames[0].Data = append(tx.Frames[0].Data, data...)
			}
			refs, err := ParseRecentRootReferences(tx.Frames[0].Data)
			require.NoError(t, err)
			count := 16
			if duplicate {
				count = 1
			}
			require.Len(t, refs, count)
			for _, ref := range refs {
				require.NoError(t, evm.IntraBlockState().SetState(accounts.InternAddress(types.FrameRecentRootVerifier), accounts.InternKey(ref.StorageKey), *new(uint256.Int).SetBytes(ref.EntryHash[:])))
			}
			tx.Frames[0].Limits.Execution = 90_000
			validation, err := ValidateFrameTransaction(evm, tx)
			require.NoError(t, err)
			require.Equal(t, refs, validation.RecentRoots)
		})
	}
}
