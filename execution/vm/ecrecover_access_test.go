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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type ecrecoverAccessReader struct {
	state.NoopReader
	code         map[accounts.Address][]byte
	accountReads map[accounts.Address]int
	codeReads    map[accounts.Address]int
}

func (r *ecrecoverAccessReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	r.accountReads[addr]++
	code, ok := r.code[addr]
	if !ok {
		return nil, nil
	}
	account := accounts.NewAccount()
	account.Nonce = 1
	account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	return &account, nil
}

func (r *ecrecoverAccessReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	r.codeReads[addr]++
	return r.code[addr], nil
}

func TestEcrecoverAccountAccess(t *testing.T) {
	fixtures, err := loadJson("ecRecover")
	require.NoError(t, err)
	index := slices.IndexFunc(fixtures, func(f precompiledTest) bool { return f.Name == "ValidKey" })
	require.NotEqual(t, -1, index)
	input, expected := common.FromHex(fixtures[index].Input), common.FromHex(fixtures[index].Expected)
	signer := accounts.InternAddress(common.BytesToAddress(expected))
	caller := accounts.InternAddress(common.HexToAddress("0x1000"))
	coinbase := accounts.InternAddress(common.HexToAddress("0x2000"))
	target := accounts.InternAddress(common.HexToAddress("0x3000"))
	precompile := accounts.InternAddress(common.HexToAddress("0x01"))
	contractCode := []byte{byte(STOP)}
	delegationCode := types.AddressToDelegation(target)
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = new(uint64)

	for _, versioned := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "versioned"}[versioned], func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				input    []byte
				code     []byte
				gas      uint64
				warm     bool
				revert   bool
				wantRead bool
				output   []byte
				err      error
			}{
				{name: "unpaid cold check", input: input, code: contractCode, gas: 6099, err: ErrOutOfGas},
				{name: "unpaid warm check", input: input, code: contractCode, gas: 3199, warm: true, err: ErrOutOfGas},
				{name: "failed recovery", code: contractCode, gas: 3000},
				{name: "absent signer", input: input, gas: 6100, wantRead: true, output: expected},
				{name: "empty code", input: input, code: []byte{}, gas: 6100, wantRead: true, output: expected},
				{name: "rejected code", input: input, code: contractCode, gas: 6100, wantRead: true},
				{name: "reverted cold read", input: input, code: contractCode, gas: 6100, wantRead: true, revert: true},
				{name: "reverted warm read", input: input, code: contractCode, gas: 3200, warm: true, wantRead: true, revert: true},
				{name: "delegation", input: input, code: delegationCode, gas: 6100, wantRead: true, output: expected},
				{name: "reverted delegation", input: input, code: delegationCode, gas: 6100, wantRead: true, output: expected, revert: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					reader := &ecrecoverAccessReader{
						code:         map[accounts.Address][]byte{target: contractCode},
						accountReads: make(map[accounts.Address]int),
						codeReads:    make(map[accounts.Address]int),
					}
					if tc.code != nil {
						reader.code[signer] = tc.code
					}
					var ibs *state.IntraBlockState
					if versioned {
						ibs = state.NewWithVersionMap(reader, state.NewVersionMap(nil))
					} else {
						ibs = state.New(reader)
					}
					defer ibs.Close()
					ibs.SetTxContext(1, 0)
					evm := NewEVM(evmtypes.BlockContext{Coinbase: coinbase}, evmtypes.TxContext{}, ibs, config, Config{})
					var accessList types.AccessList
					if tc.warm {
						accessList = types.AccessList{{Address: signer.Value()}}
					}
					ibs.Prepare(evm.ChainRules(), caller, coinbase, precompile, ActivePrecompiles(evm.ChainRules()), accessList)
					require.Zero(t, reader.accountReads[signer])
					require.Zero(t, reader.codeReads[signer])
					require.Equal(t, tc.warm, ibs.AddressInAccessList(signer))

					snapshot := ibs.PushSnapshot()
					output, _, _, err := evm.StaticCall(caller, precompile, tc.input, mdgas.MdGas{Execution: tc.gas})
					require.ErrorIs(t, err, tc.err)
					require.Equal(t, tc.output, output)
					require.Equal(t, tc.warm || tc.wantRead, ibs.AddressInAccessList(signer))
					if tc.revert {
						ibs.RevertToSnapshot(snapshot, ErrExecutionReverted)
					}
					ibs.PopSnapshot(snapshot)

					require.Equal(t, tc.wantRead, reader.accountReads[signer] > 0, "signer account read")
					require.Equal(t, tc.wantRead && len(tc.code) > 0, reader.codeReads[signer] > 0, "signer code read")
					require.Equal(t, tc.warm || tc.wantRead && !tc.revert, ibs.AddressInAccessList(signer))
					require.Zero(t, reader.accountReads[target])
					require.Zero(t, reader.codeReads[target])
					require.False(t, ibs.AddressInAccessList(target))

					io := state.NewVersionedIO(1)
					io.RecordReads(state.Version{TxIndex: 0}, ibs.VersionedReads())
					io.RecordWrites(state.Version{TxIndex: 0}, ibs.VersionedWrites())
					want := types.BlockAccessList{{Address: precompile.Value()}, {Address: coinbase.Value()}}
					if tc.wantRead {
						want = append(want, types.AccountChanges{Address: signer.Value()})
					}
					require.Equal(t, want, io.AsBlockAccessList())
				})
			}
		})
	}
}
