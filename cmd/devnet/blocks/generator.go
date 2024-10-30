// Copyright 2024 The Erigon Authors
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

package blocks

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"testing"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/v3/accounts/abi/bind"
	"github.com/erigontech/erigon/v3/accounts/abi/bind/backends"
	"github.com/erigontech/erigon/v3/core"
	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/turbo/stages/mock"
)

type TxFn func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool)

type TxGen struct {
	Fn  TxFn
	Key *ecdsa.PrivateKey
}

func GenerateBlocks(t *testing.T, gspec *types.Genesis, blocks int, txs map[int]TxGen, txPerBlock func(int) int) (*mock.MockSentry, *core.ChainPack, error) {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := mock.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, blocks, func(blockNum int, block *core.BlockGen) {
		var txn types.Transaction
		var isContractCall bool
		signer := types.LatestSignerForChainID(nil)

		txCount := txPerBlock(blockNum)

		for i := 0; i < txCount; i++ {
			if txToSend, ok := txs[i%len(txs)]; ok {
				txn, isContractCall = txToSend.Fn(block, contractBackend)
				var err error
				txn, err = types.SignTx(txn, *signer, txToSend.Key)
				if err != nil {
					return
				}
			}

			if txn != nil {
				if !isContractCall {
					err := contractBackend.SendTransaction(context.Background(), txn)
					if err != nil {
						return
					}
				}

				block.AddTx(txn)
			}
		}

		contractBackend.Commit()
	})
	if err != nil {
		return nil, nil, fmt.Errorf("generate chain: %w", err)
	}
	return m, chain, err
}
