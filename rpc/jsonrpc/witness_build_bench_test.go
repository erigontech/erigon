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
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ugorji/go/codec"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

const witnessBenchContractBytecodeHex = "361560225760005b80369010601057005b80359080602001359091556040016007565b33ff"

var witnessBenchContractBytecode = common.Hex2Bytes(witnessBenchContractBytecodeHex)

type witnessBenchRun struct {
	m             *execmoduletester.ExecModuleTester
	chain         *blockgen.ChainPack
	proofAccounts []common.Address
	proofSlots    StorageKeys
}

func BenchmarkWitnessBuild(b *testing.B) {
	b.StopTimer()
	for _, chainName := range []string{"collapse", "generated"} {
		for _, variant := range []struct {
			name string
			on   bool
		}{
			{name: "hph"},
			{name: "v3", on: true},
		} {
			restore := statecfg.SetCommitmentV3(variant.on)
			statecfg.EnableHistoricalCommitment()
			var run *witnessBenchRun
			if chainName == "collapse" {
				run = witnessBenchBuildCollapse(b, nil)
				if err := run.m.InsertChain(run.chain); err != nil {
					b.Fatal(err)
				}
			} else {
				run = witnessBenchBuildGenerated(b)
			}
			b.StartTimer()
			for _, method := range []string{"legacy", "canonical", "getWitness", "getProof"} {
				b.Run(chainName+"/"+method+"/"+variant.name, func(b *testing.B) {
					ctx := context.Background()
					debugAPI := newDebugApiForTest(run.m)
					ethAPI := newEthApiForTest(newBaseApiForTest(run.m), run.m.DB, nil, nil)
					b.ReportAllocs()
					var totalNodes, totalBytes int
					b.ResetTimer()
					for range b.N {
						for _, block := range run.chain.Blocks {
							nodes, outputBytes := witnessBenchBuildBlock(b, ctx, run, block, method, debugAPI, ethAPI)
							totalNodes += nodes
							totalBytes += outputBytes
						}
					}
					b.StopTimer()
					blocks := len(run.chain.Blocks) * b.N
					if blocks != 0 {
						b.ReportMetric(float64(totalNodes)/float64(blocks), "nodes/block")
						b.ReportMetric(float64(totalBytes)/float64(blocks), "bytes/block")
					}
				})
			}
			b.StopTimer()
			restore()
		}
	}
}

func witnessBenchBuildBlock(b *testing.B, ctx context.Context, run *witnessBenchRun, block *types.Block, method string, debugAPI *DebugAPIImpl, ethAPI *APIImpl) (int, int) {
	b.Helper()
	blockNumber := rpc.BlockNumber(block.NumberU64())
	at := rpc.BlockNumberOrHash{BlockNumber: &blockNumber}
	switch method {
	case "legacy", "canonical":
		result, err := debugAPI.ExecutionWitness(ctx, at, &method)
		if err != nil {
			b.Fatal(err)
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			b.Fatal(err)
		}
		return len(result.State), len(encoded)
	case "getWitness":
		witness, err := ethAPI.GetWitness(ctx, at)
		if err != nil {
			b.Fatal(err)
		}
		nodes, err := witnessBenchCountStreamNodes(witness)
		if err != nil {
			b.Fatal(err)
		}
		encoded, err := json.Marshal(witness)
		if err != nil {
			b.Fatal(err)
		}
		return nodes, len(encoded)
	case "getProof":
		nodes, outputBytes := 0, 0
		for _, address := range run.proofAccounts {
			proof, err := ethAPI.GetProof(ctx, address, run.proofSlots, &at)
			if err != nil {
				b.Fatal(err)
			}
			nodes += len(proof.AccountProof)
			for _, storageProof := range proof.StorageProof {
				nodes += len(storageProof.Proof)
			}
			encoded, err := json.Marshal(proof)
			if err != nil {
				b.Fatal(err)
			}
			outputBytes += len(encoded)
		}
		return nodes, outputBytes
	default:
		b.Fatalf("unknown witness benchmark method %q", method)
		return 0, 0
	}
}

var witnessBenchCbor codec.CborHandle

func witnessBenchCountStreamNodes(data []byte) (int, error) {
	reader := bytes.NewReader(data)
	if _, err := reader.ReadByte(); err != nil {
		return 0, err
	}
	decoder := codec.NewDecoder(reader, &witnessBenchCbor)
	consume := func(count int) error {
		for range count {
			var operand codec.Raw
			if err := decoder.Decode(&operand); err != nil {
				return err
			}
		}
		return nil
	}
	nodes := 0
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nodes, nil
			}
			return 0, err
		}
		switch opcode {
		case 0:
			nodes++
			if err := consume(2); err != nil {
				return 0, err
			}
		case 1:
			nodes++
			if err := consume(1); err != nil {
				return 0, err
			}
		case 2:
			nodes++
			if err := consume(1); err != nil {
				return 0, err
			}
		case 3:
			nodes++
			var hash common.Hash
			if _, err := io.ReadFull(reader, hash[:]); err != nil {
				return 0, err
			}
		case 4:
			if err := consume(1); err != nil {
				return 0, err
			}
		case 5:
			nodes++
			if err := consume(1); err != nil {
				return 0, err
			}
			flags, err := reader.ReadByte()
			if err != nil {
				return 0, err
			}
			operands := 0
			if flags&(1<<2) != 0 {
				operands++
			}
			if flags&(1<<3) != 0 {
				operands++
			}
			if flags&1 != 0 {
				operands++
			}
			if err := consume(operands); err != nil {
				return 0, err
			}
		case 6:
			nodes++
		case 0xbb:
			return nodes, nil
		default:
			return 0, fmt.Errorf("unknown witness opcode %x", opcode)
		}
	}
}

func witnessBenchBuildCollapse(tb testing.TB, emptyStorage *common.Address) *witnessBenchRun {
	tb.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		tb.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	transientBeforeSlots := witnessBenchTransientBeforeSlots
	transientAfterSlots := witnessBenchTransientAfterSlots
	cascadeSlots := witnessBenchCascadeSlots
	plainSlots := witnessBenchPlainSlots
	emptySlots := witnessBenchPlainSlots
	inlineSlots := [2]common.Hash{
		common.HexToHash("0x000000000000000000000000000000000000000000000000000000000003c6b4"),
		common.HexToHash("0x000000000000000000000000000000000000000000000000000000000007dc4f"),
	}
	if witnessBenchPrefixLength(crypto.Keccak256Hash(inlineSlots[0][:]), crypto.Keccak256Hash(inlineSlots[1][:])) < 10 {
		tb.Fatal("inline slot constants do not share the required prefix")
	}
	transientBefore := witnessBenchAddress(0x1001)
	transientAfter := witnessBenchAddress(0x1002)
	cascade := witnessBenchAddress(0x1003)
	plain := witnessBenchAddress(0x1004)
	empty := witnessBenchAddress(0x1005)
	selfDestruct := witnessBenchAddressWithPrefix(sender)
	inline := witnessBenchAddress(0x1006)
	alloc := types.GenesisAlloc{
		sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
		transientBefore: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{transientBeforeSlots[0], witnessBenchValue(1)}, [2]common.Hash{transientBeforeSlots[1], witnessBenchValue(2)},
		)},
		transientAfter: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{transientAfterSlots[0], witnessBenchValue(1)}, [2]common.Hash{transientAfterSlots[1], witnessBenchValue(2)},
		)},
		cascade: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{cascadeSlots[0], witnessBenchValue(1)}, [2]common.Hash{cascadeSlots[1], witnessBenchValue(2)}, [2]common.Hash{cascadeSlots[2], witnessBenchValue(3)},
		)},
		plain: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{plainSlots[0], witnessBenchValue(1)}, [2]common.Hash{plainSlots[1], witnessBenchValue(2)},
		)},
		empty: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{emptySlots[0], witnessBenchValue(1)}, [2]common.Hash{emptySlots[1], witnessBenchValue(2)},
		)},
		selfDestruct: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{plainSlots[0], witnessBenchValue(1)}, [2]common.Hash{plainSlots[1], witnessBenchValue(2)},
		)},
		inline: {Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: witnessBenchStorage(
			[2]common.Hash{inlineSlots[0], witnessBenchValue(1)}, [2]common.Hash{inlineSlots[1], witnessBenchValue(2)},
		)},
	}
	if emptyStorage != nil {
		alloc[*emptyStorage] = types.GenesisAccount{Balance: new(big.Int), Nonce: 1, Code: []byte{0x00}}
	}
	spec := &types.Genesis{Config: chain.TestChainBerlinConfig, Alloc: alloc, GasLimit: 10_000_000}
	m := execmoduletester.New(tb, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(key))
	witnessBenchEnableHistory(tb, m)
	generated, err := m.GenerateChain(7, func(i int, block *blockgen.BlockGen) {
		switch i {
		case 0:
			block.AddTx(witnessBenchSignedCall(tb, block, key, transientBefore, witnessBenchPairData([2]common.Hash{transientBeforeSlots[0], {}}, [2]common.Hash{transientBeforeSlots[2], witnessBenchValue(3)})))
		case 1:
			block.AddTx(witnessBenchSignedCall(tb, block, key, transientAfter, witnessBenchPairData([2]common.Hash{transientAfterSlots[0], {}}, [2]common.Hash{transientAfterSlots[2], witnessBenchValue(3)})))
		case 2:
			block.AddTx(witnessBenchSignedCall(tb, block, key, cascade, witnessBenchPairData([2]common.Hash{cascadeSlots[0], {}}, [2]common.Hash{cascadeSlots[1], {}})))
		case 3:
			block.AddTx(witnessBenchSignedCall(tb, block, key, plain, witnessBenchPairData([2]common.Hash{plainSlots[0], {}})))
		case 4:
			block.AddTx(witnessBenchSignedCall(tb, block, key, empty, witnessBenchPairData([2]common.Hash{emptySlots[0], {}}, [2]common.Hash{emptySlots[1], {}})))
		case 5:
			block.AddTx(witnessBenchSignedCall(tb, block, key, selfDestruct, nil))
		case 6:
			block.AddTx(witnessBenchSignedCall(tb, block, key, inline, witnessBenchPairData([2]common.Hash{inlineSlots[0], {}})))
		}
	})
	if err != nil {
		tb.Fatal(err)
	}
	return &witnessBenchRun{
		m: m, chain: generated,
		proofAccounts: []common.Address{sender, transientBefore, transientAfter, cascade, plain, empty, selfDestruct, inline},
		proofSlots:    witnessBenchProofSlots(),
	}
}

func witnessBenchBuildGenerated(b *testing.B) *witnessBenchRun {
	b.Helper()
	const accountCount = 200
	const contractCount = 20
	keys := make([]*ecdsa.PrivateKey, accountCount)
	addresses := make([]common.Address, accountCount)
	alloc := make(types.GenesisAlloc, accountCount+contractCount)
	for i := range keys {
		key, err := crypto.HexToECDSA(fmt.Sprintf("%064x", i+1))
		if err != nil {
			b.Fatal(err)
		}
		keys[i] = key
		addresses[i] = crypto.PubkeyToAddress(key.PublicKey)
		alloc[addresses[i]] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(24), nil)}
	}
	contracts := make([]common.Address, contractCount)
	for i := range contracts {
		address := witnessBenchAddress(0x3000 + uint64(i))
		contracts[i] = address
		storage := make(map[common.Hash]common.Hash, 64)
		for slot := range 64 {
			storage[witnessBenchValue(uint8(i*64+slot+1))] = witnessBenchValue(uint8(slot + 1))
		}
		alloc[address] = types.GenesisAccount{Balance: new(big.Int), Nonce: 1, Code: append([]byte(nil), witnessBenchContractBytecode...), Storage: storage}
	}
	spec := &types.Genesis{Config: chain.TestChainBerlinConfig, Alloc: alloc, GasLimit: 30_000_000}
	m := execmoduletester.New(b, execmoduletester.WithGenesisSpec(spec), execmoduletester.WithKey(keys[0]))
	witnessBenchEnableHistory(b, m)
	rng := rand.New(rand.NewSource(20261126))
	generated, err := m.GenerateChain(64, func(i int, block *blockgen.BlockGen) {
		selfDestruct := i%8 == 7
		storageWrites := 16
		if selfDestruct {
			storageWrites--
		}
		for range 24 {
			fromIndex := rng.Intn(accountCount)
			toIndex := rng.Intn(accountCount - 1)
			if toIndex >= fromIndex {
				toIndex++
			}
			block.AddTx(witnessBenchSignedTransfer(b, block, keys[fromIndex], addresses[toIndex]))
		}
		deadContracts := i / 8
		for txIndex := 0; txIndex < storageWrites; txIndex++ {
			contractIndex := deadContracts + rng.Intn(contractCount-deadContracts)
			slot := witnessBenchValue(uint8(contractIndex*64 + rng.Intn(64) + 1))
			value := witnessBenchValue(uint8(i + txIndex + 1))
			if rng.Intn(5) == 0 {
				value = common.Hash{}
			}
			fromIndex := rng.Intn(accountCount)
			block.AddTx(witnessBenchSignedCall(b, block, keys[fromIndex], contracts[contractIndex], witnessBenchPairData([2]common.Hash{slot, value})))
		}
		if selfDestruct {
			fromIndex := rng.Intn(accountCount)
			block.AddTx(witnessBenchSignedCall(b, block, keys[fromIndex], contracts[deadContracts], nil))
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := m.InsertChain(generated); err != nil {
		b.Fatal(err)
	}
	return &witnessBenchRun{
		m: m, chain: generated,
		proofAccounts: append(append([]common.Address{}, addresses[:4]...), contracts[:4]...),
		proofSlots:    witnessBenchProofSlots(),
	}
}

func witnessBenchEnableHistory(tb testing.TB, m *execmoduletester.ExecModuleTester) {
	tb.Helper()
	if err := m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}); err != nil {
		tb.Fatal(err)
	}
}

func witnessBenchProofSlots() StorageKeys {
	slots := make(StorageKeys, 16)
	for i := range slots {
		var slot common.Hash
		binary.BigEndian.PutUint64(slot[24:], uint64(i+1))
		slots[i] = hexutil.Bytes(slot[:])
	}
	return slots
}

func witnessBenchSignedTransfer(tb testing.TB, block *blockgen.BlockGen, key *ecdsa.PrivateKey, to common.Address) types.Transaction {
	return witnessBenchSignedTransaction(tb, block, key, to, uint64(1), 21_000, nil)
}

func witnessBenchSignedCall(tb testing.TB, block *blockgen.BlockGen, key *ecdsa.PrivateKey, to common.Address, data []byte) types.Transaction {
	return witnessBenchSignedTransaction(tb, block, key, to, uint64(1), 1_000_000, data)
}

func witnessBenchSignedTransaction(tb testing.TB, block *blockgen.BlockGen, key *ecdsa.PrivateKey, to common.Address, value uint64, gas uint64, data []byte) types.Transaction {
	tb.Helper()
	from := crypto.PubkeyToAddress(key.PublicKey)
	tx := types.NewTransaction(block.TxNonce(from), to, uint256.NewInt(value), gas, uint256.NewInt(1), data)
	signed, err := types.SignTx(tx, *types.LatestSignerForChainID(chain.TestChainBerlinConfig.ChainID), key)
	if err != nil {
		tb.Fatal(err)
	}
	return signed
}

func witnessBenchValue(value uint8) common.Hash {
	return common.Hash{31: value}
}

func witnessBenchSlot(value uint64) common.Hash {
	var slot common.Hash
	binary.BigEndian.PutUint64(slot[24:], value)
	return slot
}

func witnessBenchAddress(value uint64) common.Address {
	var address common.Address
	binary.BigEndian.PutUint64(address[12:], value)
	return address
}

func witnessBenchAddressWithPrefix(sender common.Address) common.Address {
	senderHash := crypto.Keccak256Hash(sender[:])
	for value := uint64(0x1000); ; value++ {
		candidate := witnessBenchAddress(value)
		if candidate != sender && witnessBenchNibble(crypto.Keccak256Hash(candidate[:]), 0) == witnessBenchNibble(senderHash, 0) {
			return candidate
		}
	}
}

var (
	witnessBenchTransientBeforeSlots = [3]common.Hash{witnessBenchSlot(1), witnessBenchSlot(14), witnessBenchSlot(338)}
	witnessBenchTransientAfterSlots  = [3]common.Hash{witnessBenchSlot(1), witnessBenchSlot(14), witnessBenchSlot(42)}
	witnessBenchCascadeSlots         = [3]common.Hash{witnessBenchSlot(1), witnessBenchSlot(24), witnessBenchSlot(14)}
	witnessBenchPlainSlots           = [2]common.Hash{witnessBenchSlot(1), witnessBenchSlot(2)}
)

func witnessBenchNibble(hash common.Hash, index int) byte {
	if index%2 == 0 {
		return hash[index/2] >> 4
	}
	return hash[index/2] & 0x0f
}

func witnessBenchPrefixLength(left, right common.Hash) int {
	for i := range len(left) * 2 {
		if witnessBenchNibble(left, i) != witnessBenchNibble(right, i) {
			return i
		}
	}
	return len(left) * 2
}

func witnessBenchStorage(pairs ...[2]common.Hash) map[common.Hash]common.Hash {
	storage := make(map[common.Hash]common.Hash, len(pairs))
	for _, pair := range pairs {
		storage[pair[0]] = pair[1]
	}
	return storage
}

func witnessBenchPairData(pairs ...[2]common.Hash) []byte {
	data := make([]byte, 0, len(pairs)*64)
	for _, pair := range pairs {
		data = append(data, pair[0][:]...)
		data = append(data, pair[1][:]...)
	}
	return data
}
