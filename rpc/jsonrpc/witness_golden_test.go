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
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
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

func newWitnessGoldenRun(t *testing.T) witnessGoldenRun {
	t.Helper()
	transientBeforeSlots := witnessBenchTransientBeforeSlots
	emptyStorage := witnessBenchAddress(0x1007)
	run := witnessBenchBuildCollapse(t, &emptyStorage)
	sender := run.m.Address
	transientBefore := witnessBenchAddress(0x1001)
	selfDestruct := witnessBenchAddressWithPrefix(sender)
	shapes := []witnessGoldenShape{
		{name: "transient-before", blockNum: 1},
		{name: "transient-after", blockNum: 2},
		{name: "cascade", blockNum: 3},
		{name: "plain-storage", blockNum: 4},
		{name: "empty-storage", blockNum: 5},
		{name: "self-destruct", blockNum: 6},
		{name: "inline-survivor", blockNum: 7},
	}
	absentSlot := witnessBenchValue(0x99)
	return witnessGoldenRun{
		m: run.m, chain: run.chain, shapes: shapes,
		proofs: []witnessGoldenProof{
			{address: transientBefore, storageKeys: StorageKeys{hexutil.Bytes(transientBeforeSlots[1][:])}},
			{address: transientBefore, storageKeys: StorageKeys{hexutil.Bytes(absentSlot[:])}},
			{address: sender},
			{address: witnessBenchAddress(0xdead)},
			{address: emptyStorage, storageKeys: StorageKeys{hexutil.Bytes(absentSlot[:])}},
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
	if commitmentV3 && (f.method == "eth_getWitness" || f.method == "debug_executionWitness" && f.mode == "legacy") {
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
		require.NoError(t, os.MkdirAll(*witnessGoldenDir, 0o755))
		for i, fixture := range hph {
			v3Fixture := v3[i]
			if fixture.method == "eth_getWitness" || fixture.method == "debug_executionWitness" && fixture.mode == "legacy" {
				requireWitnessGoldenFixtureSuperset(t, fixture, v3Fixture)
				if !bytes.Equal(fixture.data, v3Fixture.data) {
					v3Dir := filepath.Join(*witnessGoldenDir, "v3")
					require.NoError(t, os.MkdirAll(v3Dir, 0o755))
					require.NoError(t, os.WriteFile(filepath.Join(v3Dir, fixture.name), v3Fixture.data, 0o644))
					t.Logf("wrote v3 override %s", fixture.name)
				}
			} else {
				require.Equal(t, fixture.data, v3Fixture.data, "v3 witness fixtures must match HPH: block %d method %s mode %s", fixture.block, fixture.method, fixture.mode)
			}
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
	require.NoError(t, addDeletedAccountStorage(tx, firstTxNum, accessed))
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
		switch name {
		case "transient-before":
			require.Empty(t, hph.legacy[name], name)
			require.NotEmpty(t, v3.legacy[name], name)
		default:
			require.NotEmpty(t, hph.legacy[name], name)
			require.NotEmpty(t, v3.legacy[name], name)
		}
	}
	require.Empty(t, hph.canonical["transient-after"])
	require.Empty(t, v3.canonical["transient-after"])
}

func TestWitnessGoldenContractSStore(t *testing.T) {
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	contract := witnessBenchAddress(0x2001)
	slotOne := witnessBenchSlot(1)
	slotTwo := witnessBenchSlot(2)
	spec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			sender:   {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			contract: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(key))
	generated, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
		block.AddTx(witnessBenchSignedCall(t, block, key, contract, witnessBenchPairData([2]common.Hash{slotOne, witnessBenchValue(7)}, [2]common.Hash{slotTwo, witnessBenchValue(9)})))
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(generated))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	block := rpc.BlockNumber(1)
	code, err := api.GetCode(context.Background(), contract, bnhPtr(rpc.BlockNumberOrHashWithNumber(block)))
	require.NoError(t, err)
	require.Equal(t, witnessBenchContractBytecode, []byte(code))
	gotOne, err := api.GetStorageAt(context.Background(), contract, hexutil.Encode(slotOne[:]), bnhPtr(rpc.BlockNumberOrHashWithNumber(block)))
	require.NoError(t, err)
	require.Equal(t, witnessBenchValue(7), gotOne)
	gotTwo, err := api.GetStorageAt(context.Background(), contract, hexutil.Encode(slotTwo[:]), bnhPtr(rpc.BlockNumberOrHashWithNumber(block)))
	require.NoError(t, err)
	require.Equal(t, witnessBenchValue(9), gotTwo)
}

func TestWitnessReplayReadsOlderHeaders(t *testing.T) {
	for _, commitmentV3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("v3=%t", commitmentV3), func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(commitmentV3))
			statecfg.EnableHistoricalCommitment()
			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			contract := witnessBenchAddress(0x2002)
			spec := &types.Genesis{
				Config: chain.TestChainBerlinConfig,
				Alloc: types.GenesisAlloc{
					crypto.PubkeyToAddress(key.PublicKey): {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
					contract:                              {Balance: new(big.Int), Nonce: 1, Code: []byte{0x60, 0x02, 0x43, 0x03, 0x40, 0x60, 0x00, 0x55, 0x00}},
				},
				GasLimit: 10_000_000,
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(key))
			witnessBenchEnableHistory(t, m)
			generated, err := m.GenerateChain(3, func(i int, block *blockgen.BlockGen) {
				if i == 2 {
					getHeader := func(_ common.Hash, number uint64) (*types.Header, error) {
						return block.PrevBlock(int(number) - 1).Header(), nil
					}
					block.AddTxWithChain(getHeader, nil, witnessBenchSignedCall(t, block, key, contract, nil))
				}
			})
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(generated))
			requireCommitmentVariant(t, m, commitmentV3)
			block := generated.Blocks[2]
			mode := "legacy"
			result, err := newDebugApiForTest(m).ExecutionWitness(context.Background(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(block.NumberU64())), &mode)
			require.NoError(t, err)
			require.Len(t, result.Headers, 2)
			root, err := ReplayExecutionWitness(result, block, m.ChainConfig, m.Engine)
			require.NoError(t, err)
			require.Equal(t, block.Root(), root, "post-state root after BLOCKHASH(NUMBER-2)")
		})
	}
}

func TestWitnessGoldenChainMatchesHPH(t *testing.T) {
	var hph, v3 chainWitnesses
	var v3Chain *blockgen.ChainPack
	var v3GenesisRoot common.Hash
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
		v3Chain = run.chain
		v3GenesisRoot = run.m.Genesis.Root()
		requireGoldenChainStateless(t, run)
	}))
	require.Len(t, hph.Legacy, len(v3.Legacy))
	for n := range hph.Legacy {
		var hphLegacy, v3Legacy ExecutionWitnessResult
		require.NoError(t, json.Unmarshal(hph.Legacy[n], &hphLegacy))
		require.NoError(t, json.Unmarshal(v3.Legacy[n], &v3Legacy))
		requireWitnessStateSuperset(t, hphLegacy.State, v3Legacy.State, fmt.Sprintf("debug_executionWitness legacy, block %d", n+1))
		require.Equal(t, string(hph.Canonical[n]), string(v3.Canonical[n]), "debug_executionWitness canonical, block %d", n+1)
		requireWitnessStreamSuperset(t, hph.GetWitness[n], v3.GetWitness[n], fmt.Sprintf("eth_getWitness, block %d", n+1))
		parentRoot := v3GenesisRoot
		if n > 0 {
			parentRoot = v3Chain.Blocks[n-1].Root()
		}
		require.Equal(t, parentRoot, witnessStreamTrie(t, v3.GetWitness[n]).Hash(), "eth_getWitness parent root, block %d", n+1)
	}
}

func requireWitnessGoldenFixtureSuperset(t *testing.T, hph, v3 witnessGoldenFixture) {
	t.Helper()
	switch hph.method {
	case "debug_executionWitness":
		var hphResult, v3Result ExecutionWitnessResult
		require.NoError(t, json.Unmarshal(hph.data, &hphResult))
		require.NoError(t, json.Unmarshal(v3.data, &v3Result))
		requireWitnessStateSuperset(t, hphResult.State, v3Result.State, fmt.Sprintf("block %d method %s mode %s", hph.block, hph.method, hph.mode))
	case "eth_getWitness":
		var hphWitness, v3Witness hexutil.Bytes
		require.NoError(t, json.Unmarshal(hph.data, &hphWitness))
		require.NoError(t, json.Unmarshal(v3.data, &v3Witness))
		requireWitnessStreamSuperset(t, hphWitness, v3Witness, fmt.Sprintf("block %d method %s mode %s", hph.block, hph.method, hph.mode))
	}
}

func requireGoldenChainStateless(t *testing.T, run witnessGoldenRun) {
	t.Helper()
	debugAPI := newDebugApiForTest(run.m)
	for _, block := range run.chain.Blocks {
		for _, mode := range []string{"legacy", "canonical"} {
			result, err := debugAPI.ExecutionWitness(context.Background(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(block.NumberU64())), &mode)
			require.NoError(t, err, "block %d mode %s", block.NumberU64(), mode)
			root, err := ReplayExecutionWitness(result, block, run.m.ChainConfig, run.m.Engine)
			require.NoError(t, err, "block %d mode %s stateless replay", block.NumberU64(), mode)
			require.Equal(t, block.Root(), root, "block %d mode %s post-state root", block.NumberU64(), mode)
		}
	}
}

func TestGoldenTransientBranchChildren(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values [3]uint64
	}{{"before", [3]uint64{1, 14, 338}}, {"after", [3]uint64{1, 14, 42}}} {
		t.Run(tc.name, func(t *testing.T) {
			slots := [3]common.Hash{witnessBenchSlot(tc.values[0]), witnessBenchSlot(tc.values[1]), witnessBenchSlot(tc.values[2])}
			a := crypto.Keccak256Hash(slots[0][:])
			b := crypto.Keccak256Hash(slots[1][:])
			c := crypto.Keccak256Hash(slots[2][:])
			depth := witnessBenchPrefixLength(a, b)
			insert := witnessBenchNibble(c, depth)
			if witnessBenchPrefixLength(a, c) < depth || insert == witnessBenchNibble(a, depth) || insert == witnessBenchNibble(b, depth) {
				t.Errorf("want c in a free branch child at depth %d, got a=%x b=%x c=%x", depth, witnessBenchNibble(a, depth), witnessBenchNibble(b, depth), insert)
			}
		})
	}
}

func TestGoldenEmptyStorageSlotProof(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(*witnessGoldenDir, "1-eth_getProof.json"))
	require.NoError(t, err)
	var proofs []accounts.AccProofResult
	require.NoError(t, json.Unmarshal(data, &proofs))
	var slots int
	for _, proof := range proofs {
		if proof.StorageHash == empty.RootHash {
			slots += len(proof.StorageProof)
		}
	}
	if slots == 0 {
		t.Errorf("block 1 empty-root storage proof entries = %d, want at least one", slots)
	}
}

func TestGoldenSharedOutputOverrides(t *testing.T) {
	old := *witnessGoldenDir
	*witnessGoldenDir = t.TempDir()
	t.Cleanup(func() { *witnessGoldenDir = old })
	require.NoError(t, os.Mkdir(filepath.Join(*witnessGoldenDir, "v3"), 0o755))
	for _, fixture := range []witnessGoldenFixture{
		{name: "1-eth_getProof.json", block: 1, method: "eth_getProof", mode: "default"},
		{name: "1-debug_executionWitness-canonical.json", block: 1, method: "debug_executionWitness", mode: "canonical"},
	} {
		require.NoError(t, os.WriteFile(filepath.Join(*witnessGoldenDir, fixture.name), []byte("shared"), 0o600))
		path := filepath.Join("v3", fixture.name)
		require.NoError(t, os.WriteFile(filepath.Join(*witnessGoldenDir, path), []byte("changed"), 0o600))
		got, err := filepath.Rel(*witnessGoldenDir, witnessGoldenFixturePath(fixture, true))
		require.NoError(t, err)
		if got != fixture.name {
			t.Errorf("shared fixture %s selected %s", fixture.name, got)
		}
	}
}

func TestCollapseWitnessKeepsSurvivorStorage(t *testing.T) {
	t.Setenv("ERIGON_WITNESS_STRICT_VERIFY", "true")
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	victim := witnessBenchAddress(0x1010)
	survivor := witnessBenchAddressWithPrefix(victim)
	build := func(useV3 bool) chainWitnesses {
		t.Cleanup(statecfg.SetCommitmentV3(useV3))
		statecfg.EnableHistoricalCommitment()
		alloc := types.GenesisAlloc{
			sender:   {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			victim:   {Balance: new(big.Int), Nonce: 1, Code: witnessBenchContractBytecode},
			survivor: {Balance: new(big.Int), Nonce: 1, Code: []byte{0}, Storage: witnessBenchStorage([2]common.Hash{witnessBenchValue(1), witnessBenchValue(2)})},
		}
		spec := &types.Genesis{Config: chain.TestChainBerlinConfig, Alloc: alloc, GasLimit: 10_000_000}
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(key))
		blocks, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
			block.AddTx(witnessBenchSignedCall(t, block, key, victim, nil))
		})
		require.NoError(t, err)
		require.NoError(t, m.InsertChain(blocks))
		return collectChainWitnesses(t, useV3, m, blocks)
	}
	hph, v3 := build(false), build(true)
	t.Run("legacy", func(t *testing.T) {
		var want, got ExecutionWitnessResult
		require.NoError(t, json.Unmarshal(hph.Legacy[0], &want))
		require.NoError(t, json.Unmarshal(v3.Legacy[0], &got))
		requireWitnessStateSuperset(t, want.State, got.State, "legacy")
	})
	t.Run("canonical", func(t *testing.T) {
		want, got := hph.Canonical[0], v3.Canonical[0]
		if !bytes.Equal(want, got) {
			t.Errorf("canonical witness changed: HPH %d bytes, v3 %d bytes", len(want), len(got))
		}
	})
	t.Run("eth_getWitness", func(t *testing.T) {
		requireWitnessStreamSuperset(t, hph.GetWitness[0], v3.GetWitness[0], "eth_getWitness")
	})
}
