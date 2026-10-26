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

package executiontests

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

const pbtEdgeBlocks = 4

type pbtEdgeEnv struct {
	config *chain.Config
	key    *ecdsa.PrivateKey
	from   common.Address
}

type pbtEdgeCase struct {
	name  string
	alloc func() types.GenesisAlloc
	block func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen)
}

type pbtEdgeRun struct {
	hex    []common.Hash
	bin    []common.Hash
	allocs []types.GenesisAlloc
	failed string
}

func (e *pbtEdgeEnv) send(t *testing.T, b *blockgen.BlockGen, to *common.Address, value, gas uint64, data []byte) {
	t.Helper()
	nonce := b.TxNonce(e.from)
	gasPrice := b.GetHeader().BaseFee
	var txn types.Transaction
	if to == nil {
		txn = types.NewContractCreation(nonce, uint256.NewInt(value), gas, gasPrice, data)
	} else {
		txn = types.NewTransaction(nonce, *to, uint256.NewInt(value), gas, gasPrice, data)
	}
	signed, err := types.SignTx(txn, *types.LatestSignerForChainID(e.config.ChainID), e.key)
	require.NoError(t, err)
	b.AddTx(signed)
}

func (e *pbtEdgeEnv) sendSetCode(t *testing.T, b *blockgen.BlockGen, auth types.Authorization) {
	t.Helper()
	to := common.Address{0xbe, 0xef}
	txn := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: b.TxNonce(e.from), GasLimit: 2_000_000, To: &to},
			ChainID:  *e.config.ChainID,
			FeeCap:   *b.GetHeader().BaseFee,
		},
		Authorizations: []types.Authorization{auth},
	}
	signed, err := types.SignTx(txn, *types.LatestSignerForChainID(e.config.ChainID), e.key)
	require.NoError(t, err)
	b.AddTx(signed)
}

func edgeAddr(kind, i byte) common.Address { return common.Address{0xed, kind, i} }

var (
	edgeSelfDestructCode = common.FromHex("0x33ff")
	edgeSelfDestructInit = common.FromHex("0x6133ff6000526002601ef3")
	edgeFactory          = edgeAddr(0xa0, 0)
	edgeLiveHolder       = edgeAddr(0xa1, 0)
	edgeStorageContract  = edgeAddr(0xc0, 0)
)

func pbtEdgeCases(t *testing.T) []pbtEdgeCase {
	factoryCode := common.FromHex("0x6a" + "6133ff6000526002601ef3" + "600052600b60156000f0" + "6000600060006000" + "6001" + "855af100")
	storageCode := common.FromHex("0x600060005560003560005500")
	maxCode := 15_000
	maxInit := 2 * maxCode
	big3 := func(n int) []byte { return []byte{0x62, byte(n >> 16), byte(n >> 8), byte(n)} }
	return []pbtEdgeCase{
		{
			name: "a_selfdestruct_shared_code",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{
					edgeFactory:    {Balance: big.NewInt(1_000), Nonce: 1, Code: factoryCode},
					edgeLiveHolder: {Balance: big.NewInt(0), Nonce: 1, Code: edgeSelfDestructCode},
				}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				if i%2 == 1 {
					e.send(t, b, nil, 0, 1_000_000, edgeSelfDestructInit)
				}
				factory := edgeFactory
				e.send(t, b, &factory, 0, 1_000_000, nil)
			},
		},
		{
			name: "b_7702_set_then_clear",
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				authorityKey, err := crypto.ToECDSA(crypto.Keccak256([]byte{0x77, 0x02, byte(i)}))
				require.NoError(t, err)
				set, err := types.SignAuthorization(authorityKey, *e.config.ChainID, edgeLiveHolder, 0)
				require.NoError(t, err)
				clear, err := types.SignAuthorization(authorityKey, *e.config.ChainID, common.Address{}, 1)
				require.NoError(t, err)
				e.sendSetCode(t, b, set)
				e.sendSetCode(t, b, clear)
			},
		},
		{
			name: "c_slot_zero_and_reset",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{edgeStorageContract: {
					Balance: big.NewInt(0), Nonce: 1, Code: storageCode,
					Storage: map[common.Hash]common.Hash{{}: common.BigToHash(big.NewInt(1))},
				}}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				contract := edgeStorageContract
				values := []int64{5, 0, 7}
				if i%2 == 1 {
					values = []int64{6, 0}
				}
				for _, value := range values {
					data := common.BigToHash(big.NewInt(value))
					e.send(t, b, &contract, 0, 200_000, data[:])
				}
			},
		},
		{
			name: "d1_eip161_empty_account",
			alloc: func() types.GenesisAlloc {
				alloc := types.GenesisAlloc{}
				for i := range byte(pbtEdgeBlocks) {
					alloc[edgeAddr(0xd1, i)] = types.GenesisAccount{Balance: big.NewInt(0)}
					alloc[edgeAddr(0xd2, i)] = types.GenesisAccount{Balance: big.NewInt(0)}
				}
				return alloc
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				touchFirst, fundFirst := edgeAddr(0xd1, byte(i)), edgeAddr(0xd2, byte(i))
				e.send(t, b, &touchFirst, 0, 1_000_000, nil)
				e.send(t, b, &fundFirst, 1, 1_000_000, nil)
				e.send(t, b, &touchFirst, 1, 1_000_000, nil)
				e.send(t, b, &fundFirst, 0, 1_000_000, nil)
			},
		},
		{
			name: "d2_eip7610_storage_only_account",
			alloc: func() types.GenesisAlloc {
				alloc := types.GenesisAlloc{}
				for i := range byte(pbtEdgeBlocks) {
					alloc[edgeAddr(0xd3, i)] = types.GenesisAccount{
						Balance: big.NewInt(0),
						Storage: map[common.Hash]common.Hash{common.BigToHash(big.NewInt(1)): common.BigToHash(big.NewInt(9))},
					}
				}
				return alloc
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				address := edgeAddr(0xd3, byte(i))
				e.send(t, b, &address, 0, 1_000_000, nil)
				e.send(t, b, &address, 1, 1_000_000, nil)
			},
		},
		{
			name: "e_7002_7251_requests",
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				withdrawal := params.WithdrawalRequestAddress.Value()
				consolidation := params.ConsolidationRequestAddress.Value()
				pubkey := bytes.Repeat([]byte{byte(0x10 + i)}, 48)
				e.send(t, b, &withdrawal, 1_000, 2_000_000, append(bytes.Clone(pubkey), make([]byte, 8)...))
				e.send(t, b, &consolidation, 1_000, 2_000_000, append(bytes.Clone(pubkey), bytes.Repeat([]byte{0x40}, 48)...))
			},
		},
		{
			name: "f_large_code_under_regular_gas_cap",
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				if i%2 == 1 {
					return
				}
				runtime := make([]byte, maxCode)
				for j := range runtime {
					runtime[j] = byte(j%251 + 1)
				}
				initcode := append([]byte(nil), big3(maxCode)...)
				initcode = append(initcode, 0x60, 14, 0x5f, 0x39)
				initcode = append(initcode, big3(maxCode)...)
				initcode = append(initcode, 0x5f, 0xf3)
				require.Len(t, initcode, 14)
				initcode = append(initcode, runtime...)
				initcode = append(initcode, make([]byte, maxInit-len(initcode))...)
				e.send(t, b, nil, 0, 29_000_000, initcode)
			},
		},
	}
}

func edgeAccountDump(t *testing.T, m *execmoduletester.ExecModuleTester) types.GenesisAlloc {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	alloc := types.GenesisAlloc{}
	entry := func(address common.Address) types.GenesisAccount {
		account, ok := alloc[address]
		if !ok {
			account = types.GenesisAccount{Balance: new(big.Int)}
		}
		return account
	}
	walk := func(domain kv.Domain, visit func(k, v []byte)) {
		it, err := tx.Debug().RangeLatest(domain, nil, nil, kv.Unlim)
		require.NoError(t, err)
		defer it.Close()
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(t, err)
			if len(v) != 0 {
				visit(k, v)
			}
		}
	}
	walk(kv.AccountsDomain, func(k, v []byte) {
		var account accounts.Account
		require.NoError(t, accounts.DeserialiseV3(&account, v))
		address := common.BytesToAddress(k)
		a := entry(address)
		a.Nonce = account.Nonce
		a.Balance = account.Balance.ToBig()
		alloc[address] = a
	})
	walk(kv.CodeDomain, func(k, v []byte) {
		address := common.BytesToAddress(k)
		a := entry(address)
		a.Code = bytes.Clone(v)
		alloc[address] = a
	})
	walk(kv.StorageDomain, func(k, v []byte) {
		address := common.BytesToAddress(k[:20])
		a := entry(address)
		if a.Storage == nil {
			a.Storage = map[common.Hash]common.Hash{}
		}
		a.Storage[common.BytesToHash(k[20:])] = common.BytesToHash(v)
		alloc[address] = a
	})
	return alloc
}

func edgeBinRoot(t *testing.T, m *execmoduletester.ExecModuleTester) common.Hash {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	root, err := domains.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
	require.NoError(t, err)
	return common.BytesToHash(root)
}

func edgeGenerate(t *testing.T, m *execmoduletester.ExecModuleTester, e *pbtEdgeEnv, tc pbtEdgeCase) *blockgen.ChainPack {
	t.Helper()
	pack, err := m.GenerateChain(pbtEdgeBlocks, func(i int, b *blockgen.BlockGen) { tc.block(e, i, b) })
	require.NoError(t, err)
	for i, receipts := range pack.Receipts {
		for j, receipt := range receipts {
			require.Equal(t, uint64(1), receipt.Status, "block %d tx %d failed, gas used %d", i+1, j, receipt.GasUsed)
		}
	}
	return pack
}

func edgeOracle(t *testing.T, tc pbtEdgeCase, env *pbtEdgeEnv) *pbtEdgeRun {
	commitmentflags.Restore(t)
	previousExec3Parallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = previousExec3Parallel })
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalParallelCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	dbg.Exec3Parallel = false
	config := env.config.Copy()
	config.BinaryTrieTime = nil
	oracleEnv := &pbtEdgeEnv{config: config, key: env.key, from: env.from}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(edgeGenesis(config, env, tc)),
		execmoduletester.WithKey(env.key),
		execmoduletester.WithoutExperimentalBAL(),
	)
	pack := edgeGenerate(t, m, oracleEnv, tc)
	run := &pbtEdgeRun{}
	for i, block := range pack.Blocks {
		require.NoError(t, m.InsertChain(pack.Slice(i, i+1)))
		alloc := edgeAccountDump(t, m)
		run.allocs = append(run.allocs, alloc)
		run.hex = append(run.hex, block.Root())
	}
	return run
}

func edgeExtraAlloc(tc pbtEdgeCase) types.GenesisAlloc {
	genesis := &types.Genesis{Config: chain.AllProtocolChanges.Copy(), Alloc: types.GenesisAlloc{}}
	amsterdam := uint64(0)
	genesis.Config.AmsterdamTime = &amsterdam
	execmoduletester.AddPBTRequestContracts(genesis)
	if tc.alloc != nil {
		for address, account := range tc.alloc() {
			genesis.Alloc[address] = account
		}
	}
	return genesis.Alloc
}

func edgeGenesis(config *chain.Config, env *pbtEdgeEnv, tc pbtEdgeCase) *types.Genesis {
	alloc := types.GenesisAlloc{
		env.from:               {Balance: new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether))},
		pbtForkContractAddress: {Balance: big.NewInt(0), Nonce: 1, Code: common.FromHex("0x60003560005500")},
	}
	for address, account := range edgeExtraAlloc(tc) {
		alloc[address] = account
	}
	return &types.Genesis{Config: config, Alloc: alloc, GasLimit: 30_000_000, BaseFee: uint256.NewInt(0)}
}

func edgeDual(t *testing.T, tc pbtEdgeCase, oracle *pbtEdgeRun, bal bool) *pbtEdgeRun {
	extra := edgeExtraAlloc(tc)
	options := []execmoduletester.Option{execmoduletester.WithoutExperimentalBAL()}
	if bal {
		options = []execmoduletester.Option{}
	}
	c := newPBTForkChainWithAlloc(t, extra, options...)
	dbg.Exec3Parallel = bal
	require.NoError(t, c.m.ExecModule.ResetCurrentContext(t.Context()))
	env := &pbtEdgeEnv{config: c.config, key: c.key, from: c.from}
	pack := edgeGenerate(t, c.m, env, tc)
	roots := make([]pbtBlockRoots, len(pack.Blocks))
	for i, block := range pack.Blocks {
		oracle.bin = append(oracle.bin, pbtRootsFromAllocation(t, c.config, oracle.allocs[i], pbtForkActivationTime).bin)
		roots[i] = pbtBlockRoots{hex: oracle.hex[i], bin: oracle.bin[i]}
		if !c.config.IsBinaryTrie(block.Time()) {
			require.Equal(t, oracle.hex[i], block.Root(), "block %d: generated hex root differs from the oracle chain", i+1)
		}
	}
	setPBTRoots(pack, roots, c.config)
	run := &pbtEdgeRun{}
	for i, block := range pack.Blocks {
		if err := c.m.InsertChain(pack.Slice(i, i+1)); err != nil {
			run.failed = fmt.Sprintf("block %d (post-fork=%t) rejected: %v", i+1, c.config.IsBinaryTrie(block.Time()), err)
			t.Errorf("%s", run.failed)
			t.Logf("oracle hex root %x", oracle.hex[i])
			for address, a := range oracle.allocs[i] {
				if address[0] == 0xed {
					t.Logf("oracle dump %x nonce %d bal %v code %d storage %v", address, a.Nonce, a.Balance, len(a.Code), a.Storage)
				}
			}
			return run
		}
		alloc := edgeAccountDump(t, c.m)
		run.allocs = append(run.allocs, alloc)
		binRoot := edgeBinRoot(t, c.m)
		run.bin = append(run.bin, binRoot)
		genesisBin := pbtRootsFromAllocation(t, c.config, alloc, pbtForkActivationTime).bin
		if binRoot != genesisBin || binRoot != oracle.bin[i] {
			run.failed = fmt.Sprintf("block %d: bin root %x, GenesisToBlock(dump) %x, GenesisToBlock(oracle dump) %x", i+1, binRoot, genesisBin, oracle.bin[i])
			t.Errorf("%s", run.failed)
			t.Logf("dump vs oracle dump: %s", edgeAllocDiff(alloc, oracle.allocs[i]))
			for address, a := range alloc {
				t.Logf("dump %x nonce %d bal %v code %d storage %v", address, a.Nonce, a.Balance, len(a.Code), a.Storage)
			}
		}
	}
	return run
}

func edgeAllocDiff(a, b types.GenesisAlloc) string {
	var out []string
	for address, x := range a {
		y, ok := b[address]
		if !ok || x.Nonce != y.Nonce || x.Balance.Cmp(y.Balance) != 0 || !bytes.Equal(x.Code, y.Code) || fmt.Sprint(x.Storage) != fmt.Sprint(y.Storage) {
			out = append(out, fmt.Sprintf("%x: dual{nonce %d bal %v code %d storage %v} oracle{present %t nonce %d bal %v code %d storage %v}",
				address, x.Nonce, x.Balance, len(x.Code), x.Storage, ok, y.Nonce, y.Balance, len(y.Code), y.Storage))
		}
	}
	for address := range b {
		if _, ok := a[address]; !ok {
			out = append(out, fmt.Sprintf("%x only in oracle", address))
		}
	}
	return strings.Join(out, "; ")
}

func TestPBTEdgeStateBinaryRootMatchesFullStateRebuild(t *testing.T) {
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	amsterdam := uint64(0)
	activation := pbtForkActivationTime
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = &amsterdam
	config.BinaryTrieTime = &activation
	env := &pbtEdgeEnv{config: config, key: key, from: crypto.PubkeyToAddress(key.PublicKey)}
	for _, tc := range pbtEdgeCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			var oracle, on, off *pbtEdgeRun
			t.Run("oracle", func(t *testing.T) { oracle = edgeOracle(t, tc, env) })
			if oracle == nil || len(oracle.allocs) != pbtEdgeBlocks {
				t.FailNow()
			}
			t.Run("bal_on", func(t *testing.T) { on = edgeDual(t, tc, oracle, true) })
			t.Run("bal_off", func(t *testing.T) { off = edgeDual(t, tc, oracle, false) })
			if on == nil || off == nil {
				t.FailNow()
			}
			var lines []string
			for i := range pbtEdgeBlocks {
				equal := i < len(on.bin) && i < len(off.bin) && on.bin[i] == off.bin[i]
				lines = append(lines, fmt.Sprintf("block %d bal_on_off_equal=%t", i+1, equal))
				if !equal {
					t.Errorf("block %d: BAL-driven bin root differs from the serial one", i+1)
				}
			}
			t.Logf("%s | %s", tc.name, strings.Join(lines, " | "))
		})
	}
}
