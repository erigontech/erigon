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

package execmodule_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

const (
	selfdestructTxs      = 19
	selfdestructsPerTx   = 1950
	selfdestructTxGas    = 16_000_000
	selfdestructGasLimit = 300_000_000
)

// selfdestructCaller calls prefix+i with no value for every i in calldata [start, end).
func selfdestructCaller(prefix common.Address) []byte {
	code := []byte{
		0x60, 0x00, 0x35, // PUSH1 0 CALLDATALOAD
		0x5b,                   // loop: JUMPDEST
		0x80, 0x60, 0x20, 0x35, // DUP1 PUSH1 32 CALLDATALOAD
		0x90, 0x10, 0x15, // SWAP1 LT ISZERO
		0x60, 0x38, 0x57, // PUSH1 done JUMPI
		0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, // ret, args, value
		0x85, 0x73, // DUP6 PUSH20 prefix
	}
	code = append(code, prefix[:]...)
	return append(code,
		0x01, 0x5a, 0xf1, 0x50, // ADD GAS CALL POP
		0x60, 0x01, 0x01, // PUSH1 1 ADD
		0x60, 0x03, 0x56, // PUSH1 loop JUMP
		0x5b, 0x00, // done: JUMPDEST STOP
	)
}

// BenchmarkSelfdestructExisting validates one Amsterdam block in which every tx makes
// pre-funded existing contracts SELFDESTRUCT to their caller, the shape of the
// Benchmarkoor test_selfdestruct_existing row.
func BenchmarkSelfdestructExisting(b *testing.B) {
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(b, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	caller := benchAddress("call", 0)
	var prefix common.Address
	copy(prefix[:4], "sdc")

	var validate time.Duration
	for range b.N {
		func() {
			b.StopTimer()
			config := chain.TestChainOsakaConfig.Copy()
			config.AmsterdamTime = common.NewUint64(0)
			alloc := types.GenesisAlloc{
				sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(22), nil)},
				caller: {Code: selfdestructCaller(prefix), Balance: big.NewInt(1)},
			}
			for i := range selfdestructTxs * selfdestructsPerTx {
				alloc[benchAddress("sdc", uint64(i))] = types.GenesisAccount{Code: []byte{0x33, 0xff}, Balance: big.NewInt(1)} // CALLER SELFDESTRUCT
			}
			gspec := &types.Genesis{Config: config, GasLimit: selfdestructGasLimit, Alloc: alloc}
			m := execmoduletester.New(b, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			defer m.Close()
			signer := *types.LatestSignerForChainID(config.ChainID)
			chainResult, err := m.GenerateChain(1, func(_ int, bg *blockgen.BlockGen) {
				for t := range selfdestructTxs {
					data := make([]byte, 64)
					new(uint256.Int).SetUint64(uint64(t*selfdestructsPerTx + 1)).PutUint256(data[:32])
					new(uint256.Int).SetUint64(uint64((t+1)*selfdestructsPerTx + 1)).PutUint256(data[32:])
					txn, err := types.SignTx(types.NewTransaction(uint64(t), caller, uint256.NewInt(0), selfdestructTxGas, bg.GetHeader().BaseFee, data), signer, key)
					require.NoError(b, err)
					bg.AddTx(txn)
				}
			})
			require.NoError(b, err)
			for _, r := range chainResult.Receipts[0] {
				require.Equal(b, types.ReceiptStatusSuccessful, r.Status)
			}
			status, err := m.InsertBlocks(b.Context(), chainResult.Blocks)
			require.NoError(b, err)
			require.Equal(b, execmodule.ExecutionStatusSuccess, status)

			b.StartTimer()
			start := time.Now()
			result, err := m.ValidateChain(b.Context(), chainResult.Blocks[0])
			validate += time.Since(start)
			b.StopTimer()
			require.NoError(b, err)
			require.Equal(b, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
		}()
	}
	b.ReportMetric(float64(validate.Microseconds())/1e3/float64(b.N), "newPayload_ms")
}
