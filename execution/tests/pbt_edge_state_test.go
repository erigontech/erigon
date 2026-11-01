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
	"github.com/erigontech/erigon/execution/commitment/eip8297"
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
	check func(t *testing.T, allocs []types.GenesisAlloc)
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
	edgeStorageCode      = common.FromHex("0x600060005560003560005500")
)

func pbtEdgeCases(t *testing.T) []pbtEdgeCase {
	factoryCode := common.FromHex("0x6a" + "6133ff6000526002601ef3" + "600052600b60156000f0" + "6000600060006000" + "6001" + "855af100")
	maxCode := 15_000
	maxInit := 2 * maxCode
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
					Balance: big.NewInt(0), Nonce: 1, Code: edgeStorageCode,
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
				initcode := edgeInitcode(runtime)
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
	for _, tc := range append(pbtEdgeCases(t), pbtEdgeVectorCases(t)...) {
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
			if tc.check != nil {
				for _, run := range []*pbtEdgeRun{on, off} {
					require.Len(t, run.allocs, pbtEdgeBlocks)
					tc.check(t, run.allocs)
				}
			}
			t.Logf("%s | %s", tc.name, strings.Join(lines, " | "))
		})
	}
}

var (
	edgeSlotWriter  = edgeAddr(0xc1, 0)
	edgeSameTxZero  = edgeAddr(0xc2, 0)
	edgeFanout      = edgeAddr(0xc3, 0)
	edgeCreateFails = edgeAddr(0xc4, 0)
	edgeToucher     = edgeAddr(0xc5, 0)
)

func edgeInitcode(runtime []byte) []byte {
	n := len(runtime)
	big3 := []byte{0x62, byte(n >> 16), byte(n >> 8), byte(n)}
	initcode := append([]byte(nil), big3...)
	initcode = append(initcode, 0x60, 14, 0x5f, 0x39)
	initcode = append(initcode, big3...)
	initcode = append(initcode, 0x5f, 0xf3)
	return append(initcode, runtime...)
}

func edgeSlotData(slot, value *big.Int) []byte {
	s, v := common.BigToHash(slot), common.BigToHash(value)
	return append(s[:], v[:]...)
}

func edgeAuthority(t *testing.T, idx byte) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()
	key, err := crypto.ToECDSA(crypto.Keccak256([]byte{0x77, 0x03, idx}))
	require.NoError(t, err)
	return key, crypto.PubkeyToAddress(key.PublicKey)
}

func edgeCountCode(alloc types.GenesisAlloc, code []byte) (n int) {
	for _, a := range alloc {
		if bytes.Equal(a.Code, code) {
			n++
		}
	}
	return n
}

func edgeDelegation(target common.Address) []byte {
	return append([]byte{0xef, 0x01, 0x00}, target[:]...)
}

func pbtEdgeVectorCases(t *testing.T) []pbtEdgeCase {
	slotWriterCode := common.FromHex("0x6020356000355500")
	sameTxZeroCode := common.FromHex("0x60016000355560006000355500")
	fanoutCode := common.FromHex("0x6020355f35" + "6103e8" + "5b" + "8181" + "6005" + "02" + "55" + "82" + "90" + "03" + "80" + "6008" + "57" + "00")
	one := big.NewInt(1)
	pow := func(n uint) *big.Int { return new(big.Int).Lsh(one, n) }
	slotsG := []*big.Int{big.NewInt(63), big.NewInt(64), big.NewInt(255), big.NewInt(256), pow(255), new(big.Int).Sub(pow(256), one)}

	fill := func(n int) []byte { return bytes.Repeat([]byte{0x5b}, n) }
	pushSpan := append(append(fill(30), 0x7f), bytes.Repeat([]byte{0xaa}, 32)...)
	pushSpan = append(pushSpan, 0x5b)
	data7f := bytes.Repeat([]byte{0xbb}, 32)
	data7f[10] = 0x7f
	push7f := append(append([]byte{0x7f}, data7f...), 0x5b)
	codesI := [][]byte{fill(30), fill(31), fill(32), fill(62), fill(7936), fill(7937), append(fill(39), 0x7f), pushSpan, push7f}
	gasI := func(code []byte) uint64 { return 300_000 + 1_750*uint64(len(code)) }

	zeroChunk := append([]byte{0x5b}, make([]byte, 61)...)
	zeroChunkChunks := eip8297.ChunkifyCode(zeroChunk)
	require.Len(t, zeroChunkChunks, 2)
	require.Equal(t, [eip8297.ValueLength]byte{}, zeroChunkChunks[1])
	pushZero := append([]byte{0x7f}, make([]byte, 61)...)
	pushZeroChunks := eip8297.ChunkifyCode(pushZero)
	require.Len(t, pushZeroChunks, 2)
	require.Equal(t, byte(2), pushZeroChunks[1][0])
	require.Equal(t, [eip8297.ValueLength - 1]byte{}, [eip8297.ValueLength - 1]byte(pushZeroChunks[1][1:]))

	_, a1 := edgeAuthority(t, 1)
	_, a2 := edgeAuthority(t, 2)
	_, a3 := edgeAuthority(t, 3)
	_, a4 := edgeAuthority(t, 4)
	precompile := common.Address{19: 1}

	revertInit := common.FromHex("0x6001600055" + "60006000fd")
	oogInit := common.FromHex("0x61e0005ff3")
	require.Len(t, revertInit, 10)
	require.Len(t, oogInit, 5)
	createFailsCode := common.FromHex("0x69" + common.Bytes2Hex(revertInit) + "600052" + "600a60166000f050" + "64" + common.Bytes2Hex(oogInit) + "600052" + "6005601b6000f050" + "00")

	var touchCode []byte
	for i := range 200 {
		addr := edgeAddr(0xee, byte(i))
		for _, op := range []byte{0x31, 0x3f, 0x3b} {
			touchCode = append(append(append(touchCode, 0x73), addr[:]...), op, 0x50)
		}
		if i < 20 {
			touchCode = append(touchCode, 0x5f, 0x5f, 0x5f, 0x5f, 0x5f, 0x73)
			touchCode = append(append(touchCode, addr[:]...), 0x5a, 0xf1, 0x50)
		}
	}
	touchCode = append(touchCode, 0x00)

	return []pbtEdgeCase{
		{
			name: "g_storage_slot_boundaries",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{edgeSlotWriter: {Balance: big.NewInt(0), Nonce: 1, Code: slotWriterCode}}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				writer := edgeSlotWriter
				for _, slot := range slotsG {
					e.send(t, b, &writer, 0, 2_000_000, edgeSlotData(slot, big.NewInt(int64(i+1))))
				}
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				storage := allocs[pbtEdgeBlocks-1][edgeSlotWriter].Storage
				require.Len(t, storage, len(slotsG))
				for _, slot := range slotsG {
					require.Equal(t, common.BigToHash(big.NewInt(pbtEdgeBlocks)), storage[common.BigToHash(slot)])
				}
			},
		},
		{
			name: "h_zero_writes_to_empty_slots",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{
					edgeSlotWriter: {Balance: big.NewInt(0), Nonce: 1, Code: slotWriterCode},
					edgeSameTxZero: {Balance: big.NewInt(0), Nonce: 1, Code: sameTxZeroCode},
				}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				writer, sameTx := edgeSlotWriter, edgeSameTxZero
				for _, slot := range []int64{5, 100} {
					e.send(t, b, &writer, 0, 500_000, edgeSlotData(big.NewInt(slot), new(big.Int)))
				}
				for _, slot := range []int64{7, 200} {
					data := common.BigToHash(big.NewInt(slot))
					e.send(t, b, &sameTx, 0, 500_000, data[:])
				}
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				require.Empty(t, allocs[pbtEdgeBlocks-1][edgeSlotWriter].Storage)
				require.Empty(t, allocs[pbtEdgeBlocks-1][edgeSameTxZero].Storage)
			},
		},
		{
			name: "i_code_chunk_boundaries",
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				for _, code := range codesI {
					if len(code) == 7936 && i%2 == 1 || len(code) == 7937 && i%2 == 0 {
						continue
					}
					e.send(t, b, nil, 0, gasI(code), edgeInitcode(code))
				}
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				final := allocs[pbtEdgeBlocks-1]
				for _, code := range codesI {
					want := pbtEdgeBlocks
					if len(code) >= 7936 {
						want /= 2
					}
					require.Equal(t, want, edgeCountCode(final, code), "deployed code length %d", len(code))
				}
			},
		},
		{
			name: "j_all_zero_chunk",
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				e.send(t, b, nil, 0, 1_000_000, edgeInitcode(zeroChunk))
				e.send(t, b, nil, 0, 1_000_000, edgeInitcode(pushZero))
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				require.Equal(t, pbtEdgeBlocks, edgeCountCode(allocs[pbtEdgeBlocks-1], zeroChunk))
				require.Equal(t, pbtEdgeBlocks, edgeCountCode(allocs[pbtEdgeBlocks-1], pushZero))
			},
		},
		{
			name: "k_shared_delegation_clear",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{edgeStorageContract: {Balance: big.NewInt(0), Nonce: 1, Code: edgeStorageCode}}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				auth := func(idx byte, target common.Address, nonce uint64) {
					key, _ := edgeAuthority(t, idx)
					a, err := types.SignAuthorization(key, *e.config.ChainID, target, nonce)
					require.NoError(t, err)
					e.sendSetCode(t, b, a)
				}
				switch i {
				case 0:
					auth(1, edgeStorageContract, 0)
					auth(2, edgeStorageContract, 0)
					auth(3, precompile, 0)
					auth(4, a4, 0)
				case 1:
					auth(1, common.Address{}, 1)
				case 2:
					auth(2, common.Address{}, 1)
				}
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				shared := edgeDelegation(edgeStorageContract)
				require.Equal(t, shared, allocs[0][a1].Code)
				require.Equal(t, shared, allocs[0][a2].Code)
				require.Empty(t, allocs[1][a1].Code)
				require.Equal(t, shared, allocs[1][a2].Code)
				require.Empty(t, allocs[2][a2].Code)
				require.Equal(t, edgeDelegation(precompile), allocs[3][a3].Code)
				require.Equal(t, edgeDelegation(a4), allocs[3][a4].Code)
			},
		},
		{
			name: "l_reverted_creation",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{edgeCreateFails: {Balance: big.NewInt(0), Nonce: 1, Code: createFailsCode}}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				factory := edgeCreateFails
				e.send(t, b, &factory, 0, 3_000_000, nil)
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				final := allocs[pbtEdgeBlocks-1]
				require.Equal(t, uint64(1+2*pbtEdgeBlocks), final[edgeCreateFails].Nonce)
				for n := uint64(1); n < 1+2*pbtEdgeBlocks; n++ {
					_, ok := final[types.CreateAddress(edgeCreateFails, n)]
					require.False(t, ok, "created address for nonce %d exists", n)
				}
			},
		},
		{
			name: "m_readonly_touches",
			alloc: func() types.GenesisAlloc {
				return types.GenesisAlloc{edgeToucher: {Balance: big.NewInt(0), Nonce: 1, Code: touchCode}}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				toucher := edgeToucher
				e.send(t, b, &toucher, 0, 5_000_000, nil)
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				for i := range 200 {
					_, ok := allocs[pbtEdgeBlocks-1][edgeAddr(0xee, byte(i))]
					require.False(t, ok, "touched address %d was created", i)
				}
			},
		},
		{
			name: "n_storage_fanout",
			alloc: func() types.GenesisAlloc {
				seeded := map[common.Hash]common.Hash{}
				for n := 1; n <= 1000; n++ {
					seeded[common.BigToHash(big.NewInt(int64(5*n)))] = common.BigToHash(one)
				}
				return types.GenesisAlloc{edgeFanout: {Balance: big.NewInt(0), Nonce: 1, Code: fanoutCode, Storage: seeded}}
			},
			block: func(e *pbtEdgeEnv, i int, b *blockgen.BlockGen) {
				fanout := edgeFanout
				call := func(value, step int64) []byte {
					v, st := common.BigToHash(big.NewInt(value)), common.BigToHash(big.NewInt(step))
					return append(v[:], st[:]...)
				}
				if i > 0 {
					return
				}
				e.send(t, b, &fanout, 0, 20_000_000, call(2, 1))
				e.send(t, b, &fanout, 0, 9_000_000, call(0, 2))
			},
			check: func(t *testing.T, allocs []types.GenesisAlloc) {
				storage := allocs[pbtEdgeBlocks-1][edgeFanout].Storage
				groups := map[uint64]bool{}
				for slot := range storage {
					if n := new(big.Int).SetBytes(slot[:]).Uint64(); n >= 64 {
						groups[n>>8] = true
					}
				}
				require.Len(t, storage, 500)
				require.GreaterOrEqual(t, len(groups), 5)
				t.Logf("n_storage_fanout: %d slots in %d storage groups", len(storage), len(groups))
			},
		},
	}
}
