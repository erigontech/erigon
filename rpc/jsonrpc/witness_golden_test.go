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
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

var (
	witnessGoldenUpdate = flag.Bool("witness-golden-update", false, "update witness golden fixtures")
	witnessGoldenDir    = flag.String("witness-golden-dir", "testdata/witness_golden", "witness golden fixture directory")
)

const (
	witnessGoldenContractBytecodeHex = "361560225760005b80369010601057005b80359080602001359091556040016007565b33ff"
	witnessGoldenContractOpcodes     = "CALLDATASIZE ISZERO PUSH1 SELFDESTRUCT JUMPI PUSH1 JUMPDEST DUP1 CALLDATASIZE SWAP1 LT PUSH1 JUMPI STOP JUMPDEST DUP1 CALLDATALOAD SWAP1 DUP1 PUSH1 ADD CALLDATALOAD SWAP1 SWAP2 SSTORE PUSH1 ADD PUSH1 JUMP JUMPDEST CALLER SELFDESTRUCT"
)

var witnessGoldenContractBytecode = common.Hex2Bytes(witnessGoldenContractBytecodeHex)

type witnessGoldenShape struct {
	name     string
	blockNum uint64
}

type witnessGoldenRun struct {
	m      *execmoduletester.ExecModuleTester
	chain  *blockgen.ChainPack
	shapes []witnessGoldenShape
	proofs []witnessGoldenProof
}

type witnessGoldenProof struct {
	address     common.Address
	storageKeys StorageKeys
}

type witnessGoldenFixture struct {
	name   string
	block  uint64
	method string
	mode   string
	data   []byte
}

type witnessGoldenCandidates struct {
	legacy    map[string][][]byte
	canonical map[string][][]byte
}

type witnessGoldenHashedKey struct {
	value common.Hash
	hash  common.Hash
}

func witnessGoldenValue(value byte) common.Hash {
	return common.Hash{31: value}
}

func witnessGoldenAddress(value uint64) common.Address {
	var address common.Address
	binary.BigEndian.PutUint64(address[12:], value)
	return address
}

func witnessGoldenSlot(value uint64) witnessGoldenHashedKey {
	slot := common.Hash{}
	binary.BigEndian.PutUint64(slot[24:], value)
	return witnessGoldenHashedKey{value: slot, hash: crypto.Keccak256Hash(slot[:])}
}

func witnessGoldenNibble(hash common.Hash, index int) byte {
	if index%2 == 0 {
		return hash[index/2] >> 4
	}
	return hash[index/2] & 0x0f
}

func witnessGoldenPrefixLength(left, right common.Hash) int {
	for i := range len(left) * 2 {
		if witnessGoldenNibble(left, i) != witnessGoldenNibble(right, i) {
			return i
		}
	}
	return len(left) * 2
}

func witnessGoldenSlots(count int) []witnessGoldenHashedKey {
	slots := make([]witnessGoldenHashedKey, 0, count)
	for value := uint64(1); len(slots) < count; value++ {
		slots = append(slots, witnessGoldenSlot(value))
	}
	return slots
}

func witnessGoldenTransientSlots(t *testing.T, before bool) [3]common.Hash {
	t.Helper()
	slots := witnessGoldenSlots(4096)
	for _, deleted := range slots {
		for _, inserted := range slots {
			if deleted.value == inserted.value || witnessGoldenPrefixLength(deleted.hash, inserted.hash) != 1 {
				continue
			}
			insertedBefore := bytes.Compare(inserted.hash[:], deleted.hash[:]) < 0
			if insertedBefore != before {
				continue
			}
			for _, survivor := range slots {
				if witnessGoldenNibble(survivor.hash, 0) == witnessGoldenNibble(deleted.hash, 0) {
					continue
				}
				return [3]common.Hash{deleted.value, survivor.value, inserted.value}
			}
		}
	}
	t.Fatalf("no transient storage keys found")
	return [3]common.Hash{}
}

func witnessGoldenCascadeSlots(t *testing.T) [3]common.Hash {
	t.Helper()
	slots := witnessGoldenSlots(4096)
	for _, first := range slots {
		for _, second := range slots {
			if first.value == second.value || witnessGoldenPrefixLength(first.hash, second.hash) != 2 {
				continue
			}
			for _, survivor := range slots {
				if witnessGoldenNibble(survivor.hash, 0) != witnessGoldenNibble(first.hash, 0) || witnessGoldenNibble(survivor.hash, 1) == witnessGoldenNibble(first.hash, 1) {
					continue
				}
				return [3]common.Hash{first.value, second.value, survivor.value}
			}
		}
	}
	t.Fatalf("no cascade storage keys found")
	return [3]common.Hash{}
}

func witnessGoldenPlainSlots(t *testing.T) [2]common.Hash {
	t.Helper()
	slots := witnessGoldenSlots(256)
	for _, first := range slots {
		for _, second := range slots {
			if first.value != second.value && witnessGoldenNibble(first.hash, 0) != witnessGoldenNibble(second.hash, 0) {
				return [2]common.Hash{first.value, second.value}
			}
		}
	}
	t.Fatalf("no plain storage keys found")
	return [2]common.Hash{}
}

func witnessGoldenAddressWithPrefix(t *testing.T, sender common.Address) common.Address {
	t.Helper()
	senderHash := crypto.Keccak256Hash(sender[:])
	for value := uint64(0x1000); ; value++ {
		candidate := witnessGoldenAddress(value)
		if candidate != sender && witnessGoldenNibble(crypto.Keccak256Hash(candidate[:]), 0) == witnessGoldenNibble(senderHash, 0) {
			return candidate
		}
	}
}

func witnessGoldenStorage(pairs ...[2]common.Hash) map[common.Hash]common.Hash {
	storage := make(map[common.Hash]common.Hash, len(pairs))
	for _, pair := range pairs {
		storage[pair[0]] = pair[1]
	}
	return storage
}

func witnessGoldenPairData(pairs ...[2]common.Hash) []byte {
	data := make([]byte, 0, len(pairs)*64)
	for _, pair := range pairs {
		data = append(data, pair[0][:]...)
		data = append(data, pair[1][:]...)
	}
	return data
}

func witnessGoldenSignedCall(t *testing.T, block *blockgen.BlockGen, sender common.Address, key *ecdsa.PrivateKey, to common.Address, data []byte) types.Transaction {
	t.Helper()
	tx := types.NewTransaction(block.TxNonce(sender), to, uint256.NewInt(1), 1_000_000, uint256.NewInt(1), data)
	signer := types.LatestSignerForChainID(chain.TestChainBerlinConfig.ChainID)
	signed, err := types.SignTx(tx, *signer, key)
	require.NoError(t, err)
	return signed
}

func newWitnessGoldenRun(t *testing.T) witnessGoldenRun {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	transientBeforeSlots := witnessGoldenTransientSlots(t, true)
	transientAfterSlots := witnessGoldenTransientSlots(t, false)
	cascadeSlots := witnessGoldenCascadeSlots(t)
	plainSlots := witnessGoldenPlainSlots(t)
	emptySlots := witnessGoldenPlainSlots(t)
	inlineSlots := [2]common.Hash{
		common.HexToHash("0x000000000000000000000000000000000000000000000000000000000003c6b4"),
		common.HexToHash("0x000000000000000000000000000000000000000000000000000000000007dc4f"),
	}
	inlineHashes := [2]common.Hash{crypto.Keccak256Hash(inlineSlots[0][:]), crypto.Keccak256Hash(inlineSlots[1][:])}
	require.GreaterOrEqual(t, witnessGoldenPrefixLength(inlineHashes[0], inlineHashes[1]), 10)

	transientBefore := witnessGoldenAddress(0x1001)
	transientAfter := witnessGoldenAddress(0x1002)
	cascade := witnessGoldenAddress(0x1003)
	plain := witnessGoldenAddress(0x1004)
	empty := witnessGoldenAddress(0x1005)
	inline := witnessGoldenAddress(0x1006)
	emptyStorage := witnessGoldenAddress(0x1007)
	selfDestruct := witnessGoldenAddressWithPrefix(t, sender)
	alloc := types.GenesisAlloc{
		sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
		transientBefore: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{transientBeforeSlots[0], witnessGoldenValue(1)}, [2]common.Hash{transientBeforeSlots[1], witnessGoldenValue(2)},
		)},
		transientAfter: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{transientAfterSlots[0], witnessGoldenValue(1)}, [2]common.Hash{transientAfterSlots[1], witnessGoldenValue(2)},
		)},
		cascade: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{cascadeSlots[0], witnessGoldenValue(1)}, [2]common.Hash{cascadeSlots[1], witnessGoldenValue(2)}, [2]common.Hash{cascadeSlots[2], witnessGoldenValue(3)},
		)},
		plain: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{plainSlots[0], witnessGoldenValue(1)}, [2]common.Hash{plainSlots[1], witnessGoldenValue(2)},
		)},
		empty: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{emptySlots[0], witnessGoldenValue(1)}, [2]common.Hash{emptySlots[1], witnessGoldenValue(2)},
		)},
		selfDestruct: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{plainSlots[0], witnessGoldenValue(1)}, [2]common.Hash{plainSlots[1], witnessGoldenValue(2)},
		)},
		inline: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...), Storage: witnessGoldenStorage(
			[2]common.Hash{inlineSlots[0], witnessGoldenValue(1)}, [2]common.Hash{inlineSlots[1], witnessGoldenValue(2)},
		)},
		emptyStorage: {Balance: new(big.Int), Nonce: 1, Code: []byte{0x00}},
	}
	spec := &types.Genesis{Config: chain.TestChainBerlinConfig, Alloc: alloc, GasLimit: 10_000_000}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(key))
	shapes := []witnessGoldenShape{
		{name: "transient-before", blockNum: 1},
		{name: "transient-after", blockNum: 2},
		{name: "cascade", blockNum: 3},
		{name: "plain-storage", blockNum: 4},
		{name: "empty-storage", blockNum: 5},
		{name: "self-destruct", blockNum: 6},
		{name: "inline-survivor", blockNum: 7},
	}
	generated, err := m.GenerateChain(len(shapes), func(i int, block *blockgen.BlockGen) {
		switch i {
		case 0:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, transientBefore, witnessGoldenPairData([2]common.Hash{transientBeforeSlots[0], {}}, [2]common.Hash{transientBeforeSlots[2], witnessGoldenValue(3)})))
		case 1:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, transientAfter, witnessGoldenPairData([2]common.Hash{transientAfterSlots[0], {}}, [2]common.Hash{transientAfterSlots[2], witnessGoldenValue(3)})))
		case 2:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, cascade, witnessGoldenPairData([2]common.Hash{cascadeSlots[0], {}}, [2]common.Hash{cascadeSlots[1], {}})))
		case 3:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, plain, witnessGoldenPairData([2]common.Hash{plainSlots[0], {}})))
		case 4:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, empty, witnessGoldenPairData([2]common.Hash{emptySlots[0], {}}, [2]common.Hash{emptySlots[1], {}})))
		case 5:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, selfDestruct, nil))
		case 6:
			block.AddTx(witnessGoldenSignedCall(t, block, sender, key, inline, witnessGoldenPairData([2]common.Hash{inlineSlots[0], {}})))
		}
	})
	require.NoError(t, err)
	absentSlot := witnessGoldenValue(0x99)
	return witnessGoldenRun{
		m: m, chain: generated, shapes: shapes,
		proofs: []witnessGoldenProof{
			{address: transientBefore, storageKeys: StorageKeys{hexutil.Bytes(transientBeforeSlots[1][:])}},
			{address: transientBefore, storageKeys: StorageKeys{hexutil.Bytes(absentSlot[:])}},
			{address: sender},
			{address: witnessGoldenAddress(0xdead)},
			{address: emptyStorage},
			{address: selfDestruct},
		},
	}
}

func witnessGoldenFixturesFor(t *testing.T, commitmentV3 bool) []witnessGoldenFixture {
	t.Helper()
	previousAssert := dbg.AssertEnabled
	t.Cleanup(func() { dbg.AssertEnabled = previousAssert })
	dbg.AssertEnabled = true
	t.Cleanup(statecfg.SetCommitmentV3(commitmentV3))
	statecfg.EnableHistoricalCommitment()
	run := newWitnessGoldenRun(t)
	ctx := context.Background()
	require.NoError(t, run.m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	require.NoError(t, run.m.InsertChain(run.chain))
	requireCommitmentVariant(t, run.m, commitmentV3)

	debugAPI := newDebugApiForTest(run.m)
	ethAPI := newEthApiForTest(newBaseApiForTest(run.m), run.m.DB, nil, nil)
	fixtures := make([]witnessGoldenFixture, 0, len(run.shapes)*4)
	for _, shape := range run.shapes {
		bn := rpc.BlockNumber(shape.blockNum)
		at := rpc.BlockNumberOrHash{BlockNumber: &bn}
		for _, mode := range []string{"legacy", "canonical"} {
			result, err := debugAPI.ExecutionWitness(ctx, at, &mode)
			require.NoError(t, err, "block %d mode %s", shape.blockNum, mode)
			data, err := json.Marshal(result)
			require.NoError(t, err)
			fixtures = append(fixtures, witnessGoldenFixture{
				name: fmt.Sprintf("%d-debug_executionWitness-%s.json", shape.blockNum, mode), block: shape.blockNum, method: "debug_executionWitness", mode: mode, data: data,
			})
		}
		witness, err := ethAPI.GetWitness(ctx, at)
		require.NoError(t, err, "block %d eth_getWitness", shape.blockNum)
		data, err := json.Marshal(witness)
		require.NoError(t, err)
		fixtures = append(fixtures, witnessGoldenFixture{
			name: fmt.Sprintf("%d-eth_getWitness.json", shape.blockNum), block: shape.blockNum, method: "eth_getWitness", mode: "default", data: data,
		})

		proofs := make([]*accounts.AccProofResult, 0, len(run.proofs))
		for _, request := range run.proofs {
			proof, err := ethAPI.GetProof(ctx, request.address, request.storageKeys, &at)
			require.NoError(t, err, "block %d eth_getProof address %s", shape.blockNum, request.address)
			proofs = append(proofs, proof)
		}
		data, err = json.Marshal(proofs)
		require.NoError(t, err)
		fixtures = append(fixtures, witnessGoldenFixture{
			name: fmt.Sprintf("%d-eth_getProof.json", shape.blockNum), block: shape.blockNum, method: "eth_getProof", mode: "default", data: data,
		})
	}
	return fixtures
}

func witnessGoldenFixturePath(f witnessGoldenFixture, commitmentV3 bool) string {
	root := *witnessGoldenDir
	if commitmentV3 {
		path := filepath.Join(root, "v3", f.name)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return filepath.Join(root, f.name)
}

func TestWitnessGoldenFixtures(t *testing.T) {
	hph := witnessGoldenFixturesFor(t, false)
	v3 := witnessGoldenFixturesFor(t, true)
	if *witnessGoldenUpdate {
		require.Equal(t, hph, v3, "v3 witness fixtures must match HPH")
		require.NoError(t, os.MkdirAll(*witnessGoldenDir, 0o755))
		for _, fixture := range hph {
			require.NoError(t, os.WriteFile(filepath.Join(*witnessGoldenDir, fixture.name), fixture.data, 0o644))
		}
		return
	}
	for _, variant := range []struct {
		name string
		data []witnessGoldenFixture
		v3   bool
	}{
		{name: "hph", data: hph},
		{name: "v3", data: v3, v3: true},
	} {
		for _, fixture := range variant.data {
			path := witnessGoldenFixturePath(fixture, variant.v3)
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("witness golden fixture missing: block %d method %s mode %s: %v", fixture.block, fixture.method, fixture.mode, err)
			}
			require.Equal(t, want, fixture.data, "block %d method %s mode %s variant %s", fixture.block, fixture.method, fixture.mode, variant.name)
		}
	}
}

func witnessGoldenCandidatesFor(t *testing.T, commitmentV3 bool) witnessGoldenCandidates {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(commitmentV3))
	statecfg.EnableHistoricalCommitment()
	run := newWitnessGoldenRun(t)
	require.NoError(t, run.m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	require.NoError(t, run.m.InsertChain(run.chain))
	requireCommitmentVariant(t, run.m, commitmentV3)
	debugAPI := newDebugApiForTest(run.m)
	got := witnessGoldenCandidates{legacy: make(map[string][][]byte), canonical: make(map[string][][]byte)}
	for _, shape := range run.shapes {
		got.legacy[shape.name] = witnessGoldenCollapseCandidates(t, debugAPI, run.m, shape.blockNum, witnessModeLegacy)
		got.canonical[shape.name] = witnessGoldenCollapseCandidates(t, debugAPI, run.m, shape.blockNum, witnessModeCanonical)
	}
	return got
}

func witnessGoldenCollapseCandidates(t *testing.T, api *DebugAPIImpl, m *execmoduletester.ExecModuleTester, blockNum uint64, mode witnessMode) [][]byte {
	t.Helper()
	ctx := context.Background()
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	hash, ok, err := m.BlockReader.CanonicalHash(ctx, tx, blockNum)
	require.NoError(t, err)
	require.True(t, ok)
	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	require.NoError(t, err)
	require.NotNil(t, block)
	firstTxNum, err := api._txNumReader.Min(ctx, tx, blockNum)
	require.NoError(t, err)
	lastTxNum, err := api._txNumReader.Max(ctx, tx, blockNum)
	require.NoError(t, err)
	chainConfig, err := api.chainConfig(ctx, tx)
	require.NoError(t, err)
	accessed, _, err := api.buildAccessedState(ctx, tx, block, chainConfig, api.engine(), firstTxNum, witnessModeLegacy, nil)
	require.NoError(t, err)
	accessed.Addresses[common.Address(params.SystemAddress.Value())] = struct{}{}
	for address := range accessed.Deleted {
		to, ok := kv.NextSubtree(address[:])
		if !ok {
			to = nil
		}
		it, err := tx.RangeAsOf(kv.StorageDomain, address[:], to, firstTxNum, order.Asc, kv.Unlim)
		require.NoError(t, err)
		for it.HasNext() {
			key, value, err := it.Next()
			require.NoError(t, err)
			if len(value) == 0 {
				continue
			}
			if accessed.Storage[address] == nil {
				accessed.Storage[address] = make(map[common.Hash]struct{})
			}
			accessed.Storage[address][common.BytesToHash(key[len(address):])] = struct{}{}
		}
		it.Close()
	}
	domains, err := newSnapshotCommitmentDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	sdCtx := domains.GetCommitmentContext()
	candidates, err := detectCollapseSiblings(ctx, tx, nil, domains, sdCtx, firstTxNum, lastTxNum+1, blockNum, blockNum-1, block.Root(), accessed, mode)
	require.NoError(t, err)
	return candidates
}

func TestWitnessGoldenCollapseShapes(t *testing.T) {
	var hph, v3 witnessGoldenCandidates
	require.True(t, t.Run("hph", func(t *testing.T) { hph = witnessGoldenCandidatesFor(t, false) }))
	require.True(t, t.Run("v3", func(t *testing.T) { v3 = witnessGoldenCandidatesFor(t, true) }))
	for _, name := range []string{"transient-before", "transient-after", "cascade", "plain-storage", "empty-storage", "self-destruct", "inline-survivor"} {
		require.NotEmpty(t, hph.legacy[name], name)
		require.NotEmpty(t, v3.legacy[name], name)
		require.Equal(t, hph.legacy[name], v3.legacy[name], "legacy candidates %s", name)
		require.Equal(t, hph.canonical[name], v3.canonical[name], "canonical candidates %s", name)
	}
	require.Less(t, len(hph.canonical["transient-after"]), len(hph.legacy["transient-after"]))
	require.NotEqual(t, hph.legacy["transient-after"], hph.canonical["transient-after"])
}

func TestWitnessGoldenContractSStore(t *testing.T) {
	require.NotEmpty(t, witnessGoldenContractOpcodes)
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	contract := witnessGoldenAddress(0x2001)
	slotOne := witnessGoldenValue(1)
	slotTwo := witnessGoldenValue(2)
	spec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			sender:   {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			contract: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessGoldenContractBytecode...)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(key))
	generated, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
		block.AddTx(witnessGoldenSignedCall(t, block, sender, key, contract, witnessGoldenPairData([2]common.Hash{slotOne, witnessGoldenValue(7)}, [2]common.Hash{slotTwo, witnessGoldenValue(9)})))
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(generated))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	block := rpc.BlockNumber(1)
	code, err := api.GetCode(context.Background(), contract, bnhPtr(rpc.BlockNumberOrHashWithNumber(block)))
	require.NoError(t, err)
	require.Equal(t, witnessGoldenContractBytecode, []byte(code))
	gotOne, err := api.GetStorageAt(context.Background(), contract, hexutil.Encode(slotOne[:]), bnhPtr(rpc.BlockNumberOrHashWithNumber(block)))
	require.NoError(t, err)
	require.Equal(t, witnessGoldenValue(7), gotOne)
	gotTwo, err := api.GetStorageAt(context.Background(), contract, hexutil.Encode(slotTwo[:]), bnhPtr(rpc.BlockNumberOrHashWithNumber(block)))
	require.NoError(t, err)
	require.Equal(t, witnessGoldenValue(9), gotTwo)
}

func TestWitnessGoldenChainMatchesHPH(t *testing.T) {
	var hph, v3 chainWitnesses
	require.True(t, t.Run("hph", func(t *testing.T) {
		t.Cleanup(statecfg.SetCommitmentV3(false))
		statecfg.EnableHistoricalCommitment()
		run := newWitnessGoldenRun(t)
		require.NoError(t, run.m.InsertChain(run.chain))
		hph = collectChainWitnesses(t, false, run.m, run.chain)
	}))
	require.True(t, t.Run("v3", func(t *testing.T) {
		t.Cleanup(statecfg.SetCommitmentV3(true))
		statecfg.EnableHistoricalCommitment()
		run := newWitnessGoldenRun(t)
		require.NoError(t, run.m.InsertChain(run.chain))
		v3 = collectChainWitnesses(t, true, run.m, run.chain)
	}))
	require.Len(t, hph.Legacy, len(v3.Legacy))
	for n := range hph.Legacy {
		require.Equal(t, string(hph.Legacy[n]), string(v3.Legacy[n]), "debug_executionWitness legacy, block %d", n+1)
		require.Equal(t, string(hph.Canonical[n]), string(v3.Canonical[n]), "debug_executionWitness canonical, block %d", n+1)
		require.Equal(t, hph.GetWitness[n], v3.GetWitness[n], "eth_getWitness, block %d", n+1)
	}
}
