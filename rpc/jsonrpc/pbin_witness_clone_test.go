package jsonrpc

// What duplicated bytecode costs a binary witness.
//
// Every chunk lives in the code zone, keyed by code hash alone, so a block
// calling several clones of one contract proves one shared chunk set; distinct
// contracts of the same size prove one set each. This measures that sharing
// against hex, which stores code by hash and so ships it once either way.

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

const (
	pbinCloneCount = 8
	// One chunk past a full group, so sharing is pinned across a group boundary.
	pbinCloneChunks = pbinCodeGroupChunks + 1
	pbinCloneSize   = 31 * pbinCloneChunks
)

// pbinCloneChain deploys pbinCloneCount identical contracts and as many
// distinct ones of the same size, then calls each group in one block.
func pbinCloneChain(t *testing.T) (*pbinWitnessChain, uint64, uint64) {
	t.Helper()
	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bankAddress := crypto.PubkeyToAddress(bankKey.PublicKey)
	bankFunds, ok := new(big.Int).SetString("100000000000000000000", 10)
	require.True(t, ok)

	chainConfig := new(chain.Config)
	require.NoError(t, copier.CopyWithOption(chainConfig, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config:   chainConfig,
			Alloc:    types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
			GasLimit: 60_000_000,
		}),
		execmoduletester.WithKey(bankKey))

	signer := types.LatestSignerForChainID(nil)
	gasPrice := uint256.NewInt(1_000_000_000)
	sign := func(txn *types.LegacyTx) types.Transaction {
		t.Helper()
		txn.GasPrice = *gasPrice
		signed, err := types.SignTx(txn, *signer, bankKey)
		require.NoError(t, err)
		return signed
	}

	runtimeOf := func(distinct bool, i int) []byte {
		code := make([]byte, pbinCloneSize)
		for j := range code {
			code[j] = 0xfe
		}
		copy(code, pbinStoreRuntime)
		if distinct {
			code[len(code)-1] = byte(i)
		}
		return code
	}

	clones := make([]common.Address, pbinCloneCount)
	distinct := make([]common.Address, pbinCloneCount)
	deploys := 2 * pbinCloneCount

	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, deploys+2, func(i int, b *blockgen.BlockGen) {
		nonce := b.TxNonce(bankAddress)
		switch {
		case i < pbinCloneCount:
			clones[i] = types.CreateAddress(bankAddress, nonce)
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, GasLimit: 12_000_000, Data: pbinDeployCode(runtimeOf(false, i)),
			}}))
		case i < deploys:
			k := i - pbinCloneCount
			distinct[k] = types.CreateAddress(bankAddress, nonce)
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, GasLimit: 12_000_000, Data: pbinDeployCode(runtimeOf(true, k)),
			}}))
		default:
			targets := clones
			if i == deploys+1 {
				targets = distinct
			}
			for k := range targets {
				b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
					Nonce: b.TxNonce(bankAddress), To: &targets[k], GasLimit: 200_000,
					Data: pbinStoreCalldata(common.HexToHash("0x01"), uint64(k+1)),
				}}))
			}
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	return &pbinWitnessChain{m: m, pack: pack}, uint64(deploys + 1), uint64(deploys + 2)
}

func TestPBinWitnessCloneDedup(t *testing.T) {
	withCommitmentHistory(t)

	type row struct {
		name                       string
		hexState, hexCodes, hexTot int
		binTot, chunkB, branchB    int
		binNodes                   int
	}
	rows := []row{{name: "8 clones"}, {name: "8 distinct"}}

	t.Run("hex", func(t *testing.T) {
		c, cloneBlock, distinctBlock := pbinCloneChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i, num := range []uint64{cloneBlock, distinctBlock} {
			w := pbinWitnessOf(t, api, num)
			rows[i].hexState = sumBytes(w.State) + sumBytes(w.Headers)
			rows[i].hexCodes = sumBytes(w.Codes)
			rows[i].hexTot = rows[i].hexState + rows[i].hexCodes
		}
	})

	t.Run("bin", func(t *testing.T) {
		withBinCommitmentDatadir(t)
		c, cloneBlock, distinctBlock := pbinCloneChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i, num := range []uint64{cloneBlock, distinctBlock} {
			w := pbinWitnessOf(t, api, num)
			r := &rows[i]
			r.binNodes = len(w.State)
			r.binTot = sumBytes(w.State) + sumBytes(w.Headers)
			for _, node := range w.State {
				key := pbinLeafKeyOf(node)
				switch {
				case key == nil:
					r.branchB += len(node)
				case isCodeChunkKey(key):
					r.chunkB += len(node)
				}
			}
		}
	})

	// Direction stated up front: hex ships duplicated code once by hash, so
	// only bin's chunk sharing can keep the clone block anywhere near the
	// distinct block — clones must prove fewer bytes than distinct code.
	require.Less(t, rows[0].chunkB, rows[1].chunkB, "clones must prove fewer chunk bytes than distinct contracts")
	require.Less(t, rows[0].binTot, rows[1].binTot, "clones must prove a smaller witness than distinct contracts")
	require.Less(t, rows[0].hexCodes, rows[1].hexCodes, "hex ships duplicated code once")

	out := fmt.Sprintf("%d contracts of %d B (%d chunks, one spilling past a full group), all called in one block\n",
		pbinCloneCount, pbinCloneSize, pbinCloneChunks)
	out += fmt.Sprintf("%-12s %9s %9s %8s | %8s %7s %8s %9s %9s\n",
		"block", "hexState", "hexCodes", "hex tot", "bin tot", "/hex", "binNod", "chunkB", "branchB")
	for _, r := range rows {
		out += fmt.Sprintf("%-12s %9d %9d %8d | %8d %6.2fx %8d %9d %9d\n",
			r.name, r.hexState, r.hexCodes, r.hexTot,
			r.binTot, float64(r.binTot)/float64(r.hexTot), r.binNodes, r.chunkB, r.branchB)
	}
	t.Log("witness cost of duplicated bytecode\n" + out)
}

// TestPBinWitnessClonesProveOneChunkSet pins what content addressing was
// adopted for: accounts sharing bytecode share its chunk leaves, so the clone
// block proves exactly one chunk set and the distinct block one per contract.
func TestPBinWitnessClonesProveOneChunkSet(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	c, cloneBlock, distinctBlock := pbinCloneChain(t)
	enableCommitmentHistoryFlag(t, c.m.DB)
	api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})

	countChunks := func(num uint64) int {
		chunks := 0
		for _, node := range pbinWitnessOf(t, api, num).State {
			if key := pbinLeafKeyOf(node); key != nil && isCodeChunkKey(key) {
				chunks++
			}
		}
		return chunks
	}

	require.Equal(t, pbinCloneChunks, countChunks(cloneBlock),
		"%d clones share one content-addressed chunk set", pbinCloneCount)
	require.Equal(t, pbinCloneCount*pbinCloneChunks, countChunks(distinctBlock),
		"distinct bytecode proves one chunk set per contract")
}
