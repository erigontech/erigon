package jsonrpc

// Byte-level accounting for the EEST test_witness_growth corpus, both arms.
//
// Every measured block calls a contract that executes the same 8 bytes; only the
// dead padding behind the STOP differs. Under hex the code ships as one blob
// beside a short account proof; under bin it is committed as 31-byte chunk leaves
// inside the trie, so the same call proves every chunk the contract occupies.
//
// Bin bytes are attributed by reading each leaf's own key: the zone byte and the
// sub-index say what the leaf is, so nothing here is inferred from position.

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

// The five code sizes of tests/binary_tree/.../test_witness_growth.py.
var pbinGranCases = []struct {
	name   string
	size   int
	chunks int
}{
	{"single_chunk", 31, 1},
	{"header_full", pbinHeaderCodeCapacity, 128},
	{"first_overflow", pbinHeaderCodeCapacity + 31, 129},
	{"deep_overflow", pbinHeaderCodeCapacity * 2, 256},
	{"max_code_size", 24576, 793},
}

type pbinGranRow struct {
	name string
	// bin, by what the leaf's own key says it is
	basicData, codeHash, headerChunk, overflowChunk, storageLeaf int
	branches, binNodes, binTotal                                 int
	// hex
	hexState, hexCodes, hexNodes, hexTotal int
	headers                                int
}

// pbinGranChain deploys one contract per case, then calls each in its own block.
// Deploys come first so every measured block is a pure read of pre-existing code.
func pbinGranChain(t *testing.T) (*pbinWitnessChain, []common.Address) {
	t.Helper()
	// Own genesis rather than fundedBankGenesis: a 24,576-byte code deposit is
	// ~5M gas, past the default block limit that helper leaves on a Berlin config.
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
	addrs := make([]common.Address, len(pbinGranCases))

	sign := func(txn *types.LegacyTx) types.Transaction {
		t.Helper()
		txn.GasPrice = *gasPrice
		signed, err := types.SignTx(txn, *signer, bankKey)
		require.NoError(t, err)
		return signed
	}

	n := len(pbinGranCases)
	pack, err2 := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2*n, func(i int, b *blockgen.BlockGen) {
		nonce := b.TxNonce(bankAddress)
		if i < n { // deploy
			runtime := make([]byte, pbinGranCases[i].size)
			copy(runtime, pbinStoreRuntime)
			addrs[i] = types.CreateAddress(bankAddress, nonce)
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, GasLimit: 12_000_000, Data: pbinDeployCode(runtime),
			}}))
			return
		}
		c := i - n // call
		b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
			Nonce: nonce, To: &addrs[c], GasLimit: 200_000,
			Data: pbinStoreCalldata(common.HexToHash("0x01"), uint64(c+1)),
		}}))
	})
	require.NoError(t, err2)
	require.NoError(t, m.InsertChain(pack))
	return &pbinWitnessChain{m: m, pack: pack}, addrs
}

func TestPBinWitnessGranularity(t *testing.T) {
	withCommitmentHistory(t)
	n := len(pbinGranCases)
	rows := make([]pbinGranRow, n)
	for i := range rows {
		rows[i].name = pbinGranCases[i].name
	}

	// hex arm
	t.Run("hex", func(t *testing.T) {
		c, _ := pbinGranChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i := range pbinGranCases {
			w := pbinWitnessOf(t, api, uint64(n+i+1))
			rows[i].hexState = sumBytes(w.State)
			rows[i].hexNodes = len(w.State)
			rows[i].hexCodes = sumBytes(w.Codes)
			rows[i].headers = sumBytes(w.Headers)
			rows[i].hexTotal = rows[i].hexState + rows[i].hexCodes + rows[i].headers
		}
	})

	// bin arm
	t.Run("bin", func(t *testing.T) {
		withBinCommitmentDatadir(t)
		c, _ := pbinGranChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i := range pbinGranCases {
			w := pbinWitnessOf(t, api, uint64(n+i+1))
			r := &rows[i]
			for _, node := range w.State {
				r.binNodes++
				r.binTotal += len(node)
				key := pbinLeafKeyOf(node)
				if key == nil {
					r.branches += len(node)
					continue
				}
				switch sub := key[len(key)-1]; {
				case key[0] == 0x01:
					r.overflowChunk += len(node)
				case key[0] == 0xFF:
					r.storageLeaf += len(node)
				case sub == 0:
					r.basicData += len(node)
				case sub == 1:
					r.codeHash += len(node)
				case sub >= 128:
					r.headerChunk += len(node)
				default:
					r.storageLeaf += len(node)
				}
			}
			r.binTotal += r.headers
		}
	})

	t.Log("witness bytes for a call executing 8 bytes, by contract size\n" + pbinGranTable(rows))
}

func pbinGranTable(rows []pbinGranRow) string {
	s := fmt.Sprintf("%-16s %7s %5s %8s %9s %8s | %6s %7s %8s %8s | %7s\n",
		"case", "code B", "chunks", "hex tot", "bin tot", "bin/hex", "hexNod", "hexCode", "binNod", "chunkB", "noChunk")
	for i, r := range rows {
		chunkBytes := r.headerChunk + r.overflowChunk
		noChunk := r.binTotal - chunkBytes
		s += fmt.Sprintf("%-16s %7d %5d %8d %9d %7.2fx | %6d %7d %8d %8d | %7d\n",
			r.name, pbinGranCases[i].size, pbinGranCases[i].chunks,
			r.hexTotal, r.binTotal, float64(r.binTotal)/float64(r.hexTotal),
			r.hexNodes, r.hexCodes, r.binNodes, chunkBytes, noChunk)
	}
	s += "\nbin state bytes by what the leaf's key says it is:\n"
	s += fmt.Sprintf("%-16s %10s %9s %12s %14s %8s %10s\n",
		"case", "BASIC_DATA", "CODE_HASH", "hdr chunks", "ovf chunks", "storage", "branches")
	for _, r := range rows {
		s += fmt.Sprintf("%-16s %10d %9d %12d %14d %8d %10d\n",
			r.name, r.basicData, r.codeHash, r.headerChunk, r.overflowChunk, r.storageLeaf, r.branches)
	}
	return s
}
