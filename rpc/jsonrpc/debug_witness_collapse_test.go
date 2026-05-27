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

package jsonrpc

import (
	"context"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
)

// hashedNibbles returns the first n nibbles of the keccak hash of b.
func hashedNibbles(b []byte, n int) []byte {
	h := crypto.Keccak256Hash(b)
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			out[i] = h[i/2] >> 4
		} else {
			out[i] = h[i/2] & 0x0f
		}
	}
	return out
}

func sameNibbles(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// findSubtreeSiblingAddresses brute-forces three distinct addresses: del shares
// 2 leading hash-nibbles with sib1/sib2 but diverges at nibble 2, while sib1/sib2
// share 3 leading nibbles (forming a subtree under nibble 2). Deleting del
// collapses the depth-2 branch onto the sib1/sib2 subtree, whose root node is then
// only referenced by hash — redundant for a standard verifier. The shared first
// nibble avoids avoidFirst so the trio sits under one isolated branch. Addresses
// start above the precompile range so calls run their own code.
func findSubtreeSiblingAddresses(t *testing.T, avoidFirst map[byte]struct{}) (del, sib1, sib2 common.Address) {
	t.Helper()
	const base = uint64(1) << 20
	seen3 := map[string]common.Address{}
	var prefix3 []byte
	for c := base; c < base+10_000_000; c++ {
		var a common.Address
		binary.BigEndian.PutUint64(a[12:], c)
		nib := hashedNibbles(a.Bytes(), 3)
		if _, bad := avoidFirst[nib[0]]; bad {
			continue
		}
		key := string(nib)
		if prev, ok := seen3[key]; ok {
			require.False(t, sameNibbles(hashedNibbles(prev.Bytes(), 4), hashedNibbles(a.Bytes(), 4)),
				"the two subtree siblings must differ at nibble 3")
			sib1, sib2 = prev, a
			prefix3 = nib
			break
		}
		seen3[key] = a
	}
	require.NotNil(t, prefix3, "could not find subtree sibling pair")
	for c := base; c < base+10_000_000; c++ {
		var a common.Address
		binary.BigEndian.PutUint64(a[12:], c)
		nib := hashedNibbles(a.Bytes(), 3)
		if nib[0] == prefix3[0] && nib[1] == prefix3[1] && nib[2] != prefix3[2] {
			return a, sib1, sib2
		}
	}
	t.Fatalf("could not find sibling to delete sharing prefix %x", prefix3)
	return common.Address{}, common.Address{}, common.Address{}
}

// findSubtreeSiblingHashes is the storage-slot analogue of
// findSubtreeSiblingAddresses.
func findSubtreeSiblingHashes(t *testing.T) (del, sib1, sib2 common.Hash) {
	t.Helper()
	const base = uint64(1)
	seen3 := map[string]common.Hash{}
	var prefix3 []byte
	for c := base; c < base+10_000_000; c++ {
		var k common.Hash
		binary.BigEndian.PutUint64(k[24:], c)
		nib := hashedNibbles(k.Bytes(), 3)
		key := string(nib)
		if prev, ok := seen3[key]; ok {
			require.False(t, sameNibbles(hashedNibbles(prev.Bytes(), 4), hashedNibbles(k.Bytes(), 4)),
				"the two subtree storage siblings must differ at nibble 3")
			sib1, sib2 = prev, k
			prefix3 = nib
			break
		}
		seen3[key] = k
	}
	require.NotNil(t, prefix3, "could not find subtree storage sibling pair")
	for c := base; c < base+10_000_000; c++ {
		var k common.Hash
		binary.BigEndian.PutUint64(k[24:], c)
		nib := hashedNibbles(k.Bytes(), 3)
		if nib[0] == prefix3[0] && nib[1] == prefix3[1] && nib[2] != prefix3[2] {
			return k, sib1, sib2
		}
	}
	t.Fatalf("could not find storage slot to delete sharing prefix %x", prefix3)
	return common.Hash{}, common.Hash{}, common.Hash{}
}

// findLeafSiblingAddresses brute-forces two addresses whose hashed keys share the
// first nibble and diverge at the second, forming a 2-leaf branch at depth 1. With
// no third account under that nibble, deleting one leaf collapses the branch onto
// the other, which a standard verifier must read to promote — load-bearing, NOT
// redundant. Addresses start above the precompile range so calls run their own code.
func findLeafSiblingAddresses(t *testing.T, avoidFirst map[byte]struct{}) (del, sib common.Address) {
	t.Helper()
	const base = uint64(1) << 20
	seen := map[byte][]common.Address{}
	for c := base; c < base+10_000_000; c++ {
		var a common.Address
		binary.BigEndian.PutUint64(a[12:], c)
		nib := hashedNibbles(a.Bytes(), 2)
		if _, bad := avoidFirst[nib[0]]; bad {
			continue
		}
		for _, prev := range seen[nib[0]] {
			if hashedNibbles(prev.Bytes(), 2)[1] != nib[1] {
				return prev, a
			}
		}
		seen[nib[0]] = append(seen[nib[0]], a)
	}
	t.Fatalf("could not find 2-leaf sibling pair")
	return common.Address{}, common.Address{}
}

// selfdestructCode: PUSH1 0x00; SELFDESTRUCT (beneficiary = address 0).
var selfdestructCode = []byte{byte(vm.PUSH1), 0x00, byte(vm.SELFDESTRUCT)}

// storeZeroCode: SSTORE 0 into the slot supplied as calldata word 0; STOP.
var storeZeroCode = []byte{
	byte(vm.PUSH1), 0x00,
	byte(vm.PUSH1), 0x00,
	byte(vm.CALLDATALOAD),
	byte(vm.SSTORE),
	byte(vm.STOP),
}

var witnessTestKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

// newWitnessTester builds an ExecModuleTester with commitment history enabled and
// returns a debug API wired to it. statecfg is restored on cleanup.
func newWitnessTester(t *testing.T, gspec *types.Genesis) (*execmoduletester.ExecModuleTester, *DebugAPIImpl) {
	t.Helper()
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(witnessTestKey),
		execmoduletester.WithoutExperimentalBAL())
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	return m, api
}

func signedCall(t *testing.T, nonce uint64, to common.Address, data []byte) types.Transaction {
	t.Helper()
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: nonce, To: &to, GasLimit: 100000, Value: uint256.Int{}, Data: data},
		GasPrice: *uint256.NewInt(1),
	}, *types.LatestSignerForChainID(nil), witnessTestKey)
	require.NoError(t, err)
	return txn
}

// probeRedundantNodes reports the indices of nodes in result.State whose removal
// still lets the block verify statelessly — i.e. nodes a standard (Geth) verifier
// would not need. Index 0 (the root) is never probed. An empty result means the
// witness is minimal.
func probeRedundantNodes(t *testing.T, m *execmoduletester.ExecModuleTester, result *ExecutionWitnessResult, block *types.Block) []int {
	t.Helper()
	var removable []int
	for i := 1; i < len(result.State); i++ {
		reduced := &ExecutionWitnessResult{
			State:          make([]hexutil.Bytes, 0, len(result.State)-1),
			Codes:          result.Codes,
			Headers:        result.Headers,
			headerByNumber: result.headerByNumber,
		}
		for j, n := range result.State {
			if j == i {
				continue
			}
			reduced.State = append(reduced.State, n)
		}
		if stillVerifies(reduced, block, m) {
			removable = append(removable, i)
		}
	}
	return removable
}

// stillVerifies reports whether the block re-roots to its canonical root using the
// given (reduced) witness. A missing load-bearing node makes the verifier panic
// while descending into an unresolved hash, which counts as a failure to verify.
func stillVerifies(result *ExecutionWitnessResult, block *types.Block, m *execmoduletester.ExecModuleTester) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	root, _, err := execBlockStatelessly(result, block, m.ChainConfig, m.Engine)
	return err == nil && root == block.Root()
}

// TestExecutionWitnessCollapseSiblingAccount builds a block that selfdestructs one
// account whose hashed-key sits in a 2-child account-trie branch, the other child
// being a subtree of two untouched accounts. The delete collapses the branch onto
// the subtree, whose root the standard verifier only needs by hash. Erigon
// currently emits that subtree node as an explicit witness entry, so the witness is
// non-minimal (RED until the collapse-sibling filter lands).
func TestExecutionWitnessCollapseSiblingAccount(t *testing.T) {
	bank := crypto.PubkeyToAddress(witnessTestKey.PublicKey)
	bankNib := hashedNibbles(bank.Bytes(), 1)[0]
	del, sib1, sib2 := findSubtreeSiblingAddresses(t, map[byte]struct{}{bankNib: {}})

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			bank: {Balance: big.NewInt(1_000_000_000_000_000)},
			del:  {Code: selfdestructCode, Nonce: 1, Balance: big.NewInt(0)},
			sib1: {Balance: big.NewInt(111)},
			sib2: {Balance: big.NewInt(222)},
		},
	}
	m, api := newWitnessTester(t, gspec)
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(bank)
		b.AddTx(signedCall(t, 0, del, nil))
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	bn := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err, "block must verify statelessly (verify guardrail is on)")

	require.Empty(t, probeRedundantNodes(t, m, result, chainPack.Blocks[0]),
		"witness must be minimal: no node may be dropped while still reproducing the root")
	require.Equal(t, 5, len(result.State), "account collapse minimal witness node count")
}

// TestExecutionWitnessCollapseSiblingStorage is the storage-trie analogue: a
// contract with three slots whose hashed keys form a 2-child branch (one slot vs a
// 2-slot subtree). Zeroing the lone slot collapses the branch onto the subtree.
func TestExecutionWitnessCollapseSiblingStorage(t *testing.T) {
	bank := crypto.PubkeyToAddress(witnessTestKey.PublicKey)
	del, sib1, sib2 := findSubtreeSiblingHashes(t)
	contract := common.HexToAddress("0x00000000000000000000000000000000c0ffee01")

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			bank: {Balance: big.NewInt(1_000_000_000_000_000)},
			contract: {
				Code:    storeZeroCode,
				Nonce:   1,
				Balance: big.NewInt(0),
				Storage: map[common.Hash]common.Hash{
					del:  common.HexToHash("0x01"),
					sib1: common.HexToHash("0x02"),
					sib2: common.HexToHash("0x03"),
				},
			},
		},
	}
	m, api := newWitnessTester(t, gspec)
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(bank)
		b.AddTx(signedCall(t, 0, contract, del.Bytes()))
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	bn := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err, "block must verify statelessly (verify guardrail is on)")

	require.Empty(t, probeRedundantNodes(t, m, result, chainPack.Blocks[0]),
		"witness must be minimal: no node may be dropped while still reproducing the root")
	require.Equal(t, 7, len(result.State), "storage collapse minimal witness node count")
}

// TestExecutionWitnessCollapseSiblingAccessedSubtree is the over-filter regression
// case: a 2→1 collapse onto a 2-account subtree where the block ALSO sends value to
// one of the subtree members. The collapse still records the subtree-root branch as
// a sibling path, but that branch is now load-bearing — the verifier must descend
// through it to reach the accessed member's leaf. The filter must KEEP it; dropping
// it by the branch-node structural criterion alone corrupts the witness.
func TestExecutionWitnessCollapseSiblingAccessedSubtree(t *testing.T) {
	bank := crypto.PubkeyToAddress(witnessTestKey.PublicKey)
	bankNib := hashedNibbles(bank.Bytes(), 1)[0]
	del, sib1, sib2 := findSubtreeSiblingAddresses(t, map[byte]struct{}{bankNib: {}})

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			bank: {Balance: big.NewInt(1_000_000_000_000_000)},
			del:  {Code: selfdestructCode, Nonce: 1, Balance: big.NewInt(0)},
			sib1: {Balance: big.NewInt(111)},
			sib2: {Balance: big.NewInt(222)},
		},
	}
	m, api := newWitnessTester(t, gspec)
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(bank)
		b.AddTx(signedCall(t, 0, del, nil))
		txn, err := types.SignTx(&types.LegacyTx{
			CommonTx: types.CommonTx{Nonce: 1, To: &sib1, GasLimit: 100000, Value: *uint256.NewInt(7)},
			GasPrice: *uint256.NewInt(1),
		}, *types.LatestSignerForChainID(nil), witnessTestKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	bn := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err, "block must verify statelessly: the accessed subtree branch must be kept")

	encoded := make([][]byte, len(result.State))
	for i, n := range result.State {
		encoded[i] = n
	}
	decoded, err := trie.RLPDecode(encoded)
	require.NoError(t, err)
	sib1Node := decoded.GetNode(hashedNibbles(sib1.Bytes(), 64))
	switch sib1Node.(type) {
	case *trie.ShortNode, *trie.AccountNode:
	default:
		t.Fatalf("accessed subtree member must be reachable, got %T (filter over-dropped the load-bearing subtree branch)", sib1Node)
	}

	require.Empty(t, probeRedundantNodes(t, m, result, chainPack.Blocks[0]),
		"witness must be minimal")
}

// TestExecutionWitnessNoCollapseUnchanged exercises a block that triggers no
// collapse (a plain value transfer). The collapse-sibling filter must be a strict
// no-op here, so the witness is already minimal both before and after the change.
func TestExecutionWitnessNoCollapseUnchanged(t *testing.T) {
	bank := crypto.PubkeyToAddress(witnessTestKey.PublicKey)
	recipient := common.HexToAddress("0x00000000000000000000000000000000a11ce001")

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			bank:      {Balance: big.NewInt(1_000_000_000_000_000)},
			recipient: {Balance: big.NewInt(1)},
		},
	}
	m, api := newWitnessTester(t, gspec)
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(bank)
		txn, err := types.SignTx(&types.LegacyTx{
			CommonTx: types.CommonTx{Nonce: 0, To: &recipient, GasLimit: 100000, Value: *uint256.NewInt(1000)},
			GasPrice: *uint256.NewInt(1),
		}, *types.LatestSignerForChainID(nil), witnessTestKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	bn := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err)

	require.Empty(t, probeRedundantNodes(t, m, result, chainPack.Blocks[0]),
		"a no-collapse witness is already minimal; the filter must not change it")
}

// TestExecutionWitnessCollapseSiblingLoadBearing is the over-filter counter-case:
// a genuine 2-leaf 2→1 collapse whose surviving sibling is a ShortNode leaf, not a
// branch subtree. The standard verifier must read that leaf's preimage to promote it
// on collapse, so the filter must KEEP it. The test asserts the specific surviving
// sibling resolves to a concrete leaf node present in the witness (not a bare
// HashNode) — a degenerate "drop everything matching a sibling path" filter would
// drop it and fail here.
func TestExecutionWitnessCollapseSiblingLoadBearing(t *testing.T) {
	bank := crypto.PubkeyToAddress(witnessTestKey.PublicKey)
	bankNib := hashedNibbles(bank.Bytes(), 1)[0]
	del, sib := findLeafSiblingAddresses(t, map[byte]struct{}{bankNib: {}})

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			bank: {Balance: big.NewInt(1_000_000_000_000_000)},
			del:  {Code: selfdestructCode, Nonce: 1, Balance: big.NewInt(0)},
			sib:  {Balance: big.NewInt(111)},
		},
	}
	m, api := newWitnessTester(t, gspec)
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(bank)
		b.AddTx(signedCall(t, 0, del, nil))
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	bn := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err, "load-bearing sibling must verify: dropping it would fail verification")

	encoded := make([][]byte, len(result.State))
	for i, n := range result.State {
		encoded[i] = n
	}
	decoded, err := trie.RLPDecode(encoded)
	require.NoError(t, err)

	sibNode := decoded.GetNode(hashedNibbles(sib.Bytes(), 64))
	switch sibNode.(type) {
	case *trie.ShortNode, *trie.AccountNode:
	default:
		t.Fatalf("surviving sibling must be kept as a resolved leaf, got %T (filter over-dropped a load-bearing node)", sibNode)
	}

	require.Empty(t, probeRedundantNodes(t, m, result, chainPack.Blocks[0]),
		"load-bearing collapse witness must be minimal: the kept sibling is needed, nothing else is redundant")
	require.Equal(t, 5, len(result.State), "load-bearing collapse witness node count")
}
