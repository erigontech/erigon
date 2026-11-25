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
	"strings"
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

// BenchmarkCreateZeroBytes validates one Amsterdam block of a few txs that each run many empty
// CREATEs, the shape of the Benchmarkoor test_create row (0 bytes, no value, opcode CREATE).
func BenchmarkCreateZeroBytes(b *testing.B) {
	const txs, createsPerTx = 6, 45
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(b, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	creator := benchAddress("crea", 0)
	alloc := types.GenesisAlloc{
		sender:  {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(22), nil)},
		creator: {Code: common.FromHex(strings.Repeat("600060006000f050", createsPerTx) + "00")}, // CREATE(0, 0, 0) POP
	}

	var validate time.Duration
	for range b.N {
		func() {
			b.StopTimer()
			m := execmoduletester.New(b, execmoduletester.WithKey(key), execmoduletester.WithExperimentalBAL(),
				execmoduletester.WithGenesisSpec(&types.Genesis{Config: chain.AllProtocolChanges, GasLimit: 60_000_000, Alloc: alloc}))
			defer m.Close()
			signer := *types.LatestSignerForChainID(nil)
			chainResult, err := m.GenerateChain(1, func(_ int, bg *blockgen.BlockGen) {
				for range txs {
					txn, err := types.SignTx(types.NewTransaction(bg.TxNonce(sender), creator, uint256.NewInt(0), 9_000_000, bg.GetHeader().BaseFee, nil), signer, key)
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
	b.ReportMetric(float64(validate.Nanoseconds())/float64(b.N)/1e6, "newPayload_ms")
}
