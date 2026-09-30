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

package execmoduletester

import (
	"bytes"
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

type PBTAcceptanceChain struct {
	Tester   *ExecModuleTester
	Chain    *blockgen.ChainPack
	Genesis  *types.Genesis
	Sender   common.Address
	Contract common.Address
}

func NewPBTAcceptanceChain(tb testing.TB, binary bool, dual bool) (*PBTAcceptanceChain, error) {
	tb.Helper()
	config := chain.TestChainBerlinConfig.Copy()
	if binary {
		config = chain.AllProtocolChanges.Copy()
		amsterdam := uint64(0)
		config.AmsterdamTime = &amsterdam
		zero := uint64(0)
		config.BinaryTrieTime = &zero
	} else {
		config.BinaryTrieTime = nil
	}
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		return nil, err
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	contract := common.Address{6}
	sharedCode := bytes.Repeat([]byte{1}, 32)
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{7}, 20)...)
	genesis := &types.Genesis{
		Config:   config,
		Alloc:    acceptanceAlloc(sender, contract, sharedCode, delegation),
		GasLimit: 30_000_000,
		BaseFee:  uint256.NewInt(0),
	}
	dirs := datadir.New(tb.TempDir())
	options := []Option{WithGenesisSpec(genesis), WithKey(key), WithStepSize(1), WithDataDir(dirs)}
	if dual {
		options = append(options, WithEnableDomain(kv.CommitmentBinDomain))
	}
	tester := New(tb, options...)
	{
		tx, txErr := tester.DB.BeginTemporalRw(tb.Context())
		if txErr != nil {
			tester.Close()
			return nil, txErr
		}
		defer tx.Rollback()
		if config.IsAmsterdam(0) {
			for _, address := range []common.Address{
				config.GetBuilderDepositContract().Value(),
				config.GetBuilderExitContract().Value(),
				config.GetWithdrawalRequestContract().Value(),
				config.GetConsolidationRequestContract().Value(),
			} {
				code, _, codeErr := tx.GetLatest(kv.CodeDomain, address[:], kv.GetLatestOptions{})
				if codeErr != nil {
					tx.Rollback()
					tester.Close()
					return nil, codeErr
				}
				if len(code) == 0 {
					switch address {
					case config.GetBuilderDepositContract().Value():
						code = misc.BuilderDepositRequestCode
					case config.GetBuilderExitContract().Value():
						code = misc.BuilderExitRequestCode
					}
				}
				if len(code) > 0 {
					genesis.Alloc[address] = types.GenesisAccount{Nonce: 1, Code: bytes.Clone(code)}
				}
			}
			if deleteErr := tx.Delete(kv.ConfigTable, kv.GenesisKey); deleteErr != nil {
				tx.Rollback()
				tester.Close()
				return nil, deleteErr
			}
			if writeErr := rawdb.WriteGenesisIfNotExist(tx, genesis); writeErr != nil {
				tx.Rollback()
				tester.Close()
				return nil, writeErr
			}
		}
		var commitErr error
		if binary || dual {
			domains, domainErr := execctx.NewSharedDomains(tb.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
			if domainErr != nil {
				tx.Rollback()
				tester.Close()
				return nil, domainErr
			}
			_, ibs, computeErr := genesiswrite.ComputeGenesisCommitment(tb.Context(), genesis, tx, domains, tester.Genesis.Header())
			if computeErr != nil {
				commitErr = computeErr
			} else {
				ibs.Close()
				commitErr = domains.Commit(tb.Context(), tx)
			}
			domains.Close()
		}
		if commitErr == nil {
			commitErr = tx.Commit()
		} else {
			tx.Rollback()
		}
		if commitErr != nil {
			tester.Close()
			return nil, commitErr
		}
		if binary || dual {
			if resetErr := tester.ExecModule.ResetCurrentContext(tb.Context()); resetErr != nil {
				tester.Close()
				return nil, resetErr
			}
		}
	}
	baseAlloc := copyGenesisAlloc(genesis.Alloc)
	pack, err := tester.GenerateChain(4, func(i int, b *blockgen.BlockGen) {
		data := common.BigToHash(big.NewInt(int64(i + 1)))
		tx, txErr := types.SignTx(types.NewTransaction(uint64(i), contract, uint256.NewInt(0), 2_000_000, uint256.NewInt(0), data[:]), *types.LatestSignerForChainID(config.ChainID), key)
		if txErr != nil {
			tb.Fatal(txErr)
		}
		b.AddTx(tx)
	})
	if err != nil {
		tester.Close()
		return nil, err
	}
	if binary {
		for i, block := range pack.Blocks {
			alloc := copyGenesisAlloc(baseAlloc)
			for j := 0; j <= i; j++ {
				senderAccount := copyGenesisAccount(alloc[sender])
				senderAccount.Nonce++
				alloc[sender] = senderAccount
				contractAccount := copyGenesisAccount(alloc[contract])
				contractAccount.Storage[common.Hash{}] = common.BigToHash(big.NewInt(int64(j + 1)))
				alloc[contract] = contractAccount
			}
			exit := config.GetBuilderExitContract().Value()
			account := alloc[exit]
			account.Storage = nil
			alloc[exit] = account
			rootBlock, state, rootErr := genesiswrite.GenesisToBlock(&types.Genesis{Config: config, Difficulty: uint256.NewInt(0), Alloc: alloc}, datadir.New(tb.TempDir()), log.New())
			if rootErr != nil {
				tester.Close()
				return nil, rootErr
			}
			state.Close()
			header := block.Header()
			if i > 0 {
				header.ParentHash = pack.Blocks[i-1].Hash()
			}
			header.Root = rootBlock.Root()
			pack.Blocks[i] = block.WithSeal(header)
			pack.Headers[i] = pack.Blocks[i].HeaderNoCopy()
		}
		pack.TopBlock = pack.Blocks[len(pack.Blocks)-1]
	}
	return &PBTAcceptanceChain{Tester: tester, Chain: pack, Genesis: genesis, Sender: sender, Contract: contract}, nil
}

func acceptanceAlloc(sender, contract common.Address, sharedCode, delegation []byte) types.GenesisAlloc {
	return types.GenesisAlloc{
		sender:            {Balance: new(big.Int).Mul(big.NewInt(100), new(big.Int).SetUint64(common.Ether))},
		common.Address{1}: {Nonce: 1, Balance: big.NewInt(2), Storage: map[common.Hash]common.Hash{{}: common.BigToHash(big.NewInt(3)), common.BytesToHash([]byte{0x80}): common.BigToHash(big.NewInt(4))}},
		common.Address{2}: {Code: bytes.Clone(sharedCode)},
		common.Address{3}: {Code: bytes.Clone(sharedCode)},
		common.Address{4}: {Code: make([]byte, 31)},
		common.Address{5}: {Code: bytes.Clone(delegation)},
		contract:          {Code: common.FromHex("0x60003560005500"), Storage: map[common.Hash]common.Hash{}},
	}
}

func copyGenesisAlloc(source types.GenesisAlloc) types.GenesisAlloc {
	copy := make(types.GenesisAlloc, len(source))
	for address, account := range source {
		copy[address] = copyGenesisAccount(account)
	}
	return copy
}

func copyGenesisAccount(account types.GenesisAccount) types.GenesisAccount {
	copy := account
	if account.Balance != nil {
		copy.Balance = new(big.Int).Set(account.Balance)
	}
	copy.Code = bytes.Clone(account.Code)
	copy.Storage = make(map[common.Hash]common.Hash, len(account.Storage))
	maps.Copy(copy.Storage, account.Storage)
	return copy
}
