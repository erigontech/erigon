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
	"crypto/ecdsa"
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

type PBTAcceptanceChain struct {
	Tester   *ExecModuleTester
	Chain    *blockgen.ChainPack
	Genesis  *types.Genesis
	Key      *ecdsa.PrivateKey
	Sender   common.Address
	Contract common.Address
}

func NewPBTAcceptanceChain(tb testing.TB, binary bool, dual bool) (*PBTAcceptanceChain, error) {
	return NewPBTAcceptanceChainWithSharedCode(tb, binary, dual, bytes.Repeat([]byte{1}, 32))
}

func NewPBTAcceptanceChainWithSharedCode(tb testing.TB, binary bool, dual bool, sharedCode []byte) (*PBTAcceptanceChain, error) {
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
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{7}, 20)...)
	genesis := &types.Genesis{
		Config:   config,
		Alloc:    acceptanceAlloc(sender, contract, sharedCode, delegation),
		GasLimit: 30_000_000,
		BaseFee:  uint256.NewInt(0),
	}
	addAmsterdamBuilderContracts(genesis)
	addPBTRequestContracts(genesis)
	dirs := datadir.New(tb.TempDir())
	refs := false
	variant := dbstate.TrieVariantHex
	settings := &dbstate.ErigonDBSettings{StepSize: 1, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs, TrieVariant: &variant}
	if binary {
		variant = dbstate.TrieVariantBin
	}
	if dual {
		variant = dbstate.TrieVariantHexBin
	}
	if binary || dual {
		hash := statecfg.BinCommitmentHash
		if hash == "" {
			hash = commitment.PBinHashBlake3
		}
		settings.TrieHash = &hash
		previousParallel := statecfg.ExperimentalParallelCommitment
		statecfg.ExperimentalParallelCommitment = false
		tb.Cleanup(func() { statecfg.ExperimentalParallelCommitment = previousParallel })
	}
	if err := dbstate.WriteErigonDBSettings(dirs, settings); err != nil {
		return nil, err
	}
	options := []Option{WithGenesisSpec(genesis), WithKey(key), WithStepSize(1), WithDataDir(dirs)}
	if dual {
		previousBin := statecfg.ExperimentalBinCommitment
		previousHexBin := statecfg.ExperimentalHexBinCommitment
		previousV3 := statecfg.ExperimentalCommitmentV3
		previousSchema := statecfg.Schema
		statecfg.ExperimentalBinCommitment = true
		statecfg.ExperimentalHexBinCommitment = true
		statecfg.ExperimentalCommitmentV3 = true
		statecfg.InitSchemas()
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
		tb.Cleanup(func() {
			statecfg.ExperimentalBinCommitment = previousBin
			statecfg.ExperimentalHexBinCommitment = previousHexBin
			statecfg.ExperimentalCommitmentV3 = previousV3
			statecfg.Schema = previousSchema
		})
		options = append(options, WithEnableDomain(kv.CommitmentBinDomain))
	}
	tester := New(tb, options...)
	settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
	if settingsErr != nil {
		tester.Close()
		return nil, settingsErr
	}
	settings.StepSize = 1
	settings.StepsInFrozenFile = 1
	if writeErr := dbstate.WriteErigonDBSettings(dirs, settings); writeErr != nil {
		tester.Close()
		return nil, writeErr
	}
	{
		tx, txErr := tester.DB.BeginTemporalRw(tb.Context())
		if txErr != nil {
			tester.Close()
			return nil, txErr
		}
		defer tx.Rollback()
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
	return &PBTAcceptanceChain{Tester: tester, Chain: pack, Genesis: genesis, Key: key, Sender: sender, Contract: contract}, nil
}

func addPBTRequestContracts(genesis *types.Genesis) {
	if genesis.Config.AmsterdamTime == nil {
		return
	}
	genesis.Alloc[genesis.Config.GetWithdrawalRequestContract().Value()] = types.GenesisAccount{
		Balance: new(big.Int),
		Code:    common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd"),
		Nonce:   1,
	}
	genesis.Alloc[genesis.Config.GetConsolidationRequestContract().Value()] = types.GenesisAccount{
		Balance: new(big.Int),
		Code:    common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd"),
		Nonce:   1,
	}
}

func acceptanceAlloc(sender, contract common.Address, sharedCode, delegation []byte) types.GenesisAlloc {
	return types.GenesisAlloc{
		sender:            {Balance: new(big.Int).Mul(big.NewInt(100), new(big.Int).SetUint64(common.Ether))},
		common.Address{1}: {Nonce: 1, Balance: big.NewInt(2), Storage: map[common.Hash]common.Hash{{}: common.BigToHash(big.NewInt(3)), common.BytesToHash([]byte{0x80}): common.BigToHash(big.NewInt(4))}},
		common.Address{2}: {Balance: new(big.Int), Code: bytes.Clone(sharedCode)},
		common.Address{3}: {Balance: new(big.Int), Code: bytes.Clone(sharedCode)},
		common.Address{4}: {Balance: new(big.Int), Code: make([]byte, 31)},
		common.Address{5}: {Balance: new(big.Int), Code: bytes.Clone(delegation)},
		contract:          {Balance: new(big.Int), Code: common.FromHex("0x60003560005500"), Storage: map[common.Hash]common.Hash{}},
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
