package blocks

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
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
