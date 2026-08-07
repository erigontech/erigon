package jsonrpc

// What duplicated bytecode costs a binary witness.
//
// A contract's first 128 chunks live in its own account header, keyed by
// address; everything past that lives in the code zone, keyed by code hash. So
// a block calling several clones of one contract proves the shared tail once
// and the per-account head once per clone. This measures that split against
// hex, which stores code by hash and so ships it once either way.

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
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

const (
	pbinCloneCount = 8
	// 256 chunks: 128 in the account header, 128 in the code zone.
	pbinCloneSize = pbinHeaderCodeCapacity * 2
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
		binTot, hdrChunk, ovfChunk int
		binBranch, binNodes        int
		zonedTot                   int // header chunks re-pruned away: what the code zone alone carries
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
			nodes := make([][]byte, 0, len(w.State))
			keep := make([][]byte, 0, len(w.State))
			for _, node := range w.State {
				nodes = append(nodes, node)
				key := pbinLeafKeyOf(node)
				switch {
				case key == nil:
					r.binBranch += len(node)
					continue
				case key[0] == 0x01:
					r.ovfChunk += len(node)
				case key[len(key)-1] >= 128:
					r.hdrChunk += len(node)
					continue // the chunks the proposal would move out of the header
				}
				keep = append(keep, key)
			}
			root := c.block(t, num-1).Root()
			lean, err := commitment.PBinWitnessNodesForKeys(nodes, root[:], keep)
			require.NoError(t, err)
			r.zonedTot = sumBytes(w.Headers)
			for _, node := range lean {
				r.zonedTot += len(node)
			}
		}
	})

	out := fmt.Sprintf("%d contracts of %d B (%d chunks: 128 header, 128 code zone), all called in one block\n",
		pbinCloneCount, pbinCloneSize, pbinCloneSize/31)
	out += fmt.Sprintf("%-12s %9s %9s %8s | %8s %7s %9s %9s %9s | %9s\n",
		"block", "hexState", "hexCodes", "hex tot", "bin tot", "/hex", "hdrChunkB", "ovfChunkB", "branchB", "zoneOnly")
	for _, r := range rows {
		out += fmt.Sprintf("%-12s %9d %9d %8d | %8d %6.2fx %9d %9d %9d | %9d\n",
			r.name, r.hexState, r.hexCodes, r.hexTot,
			r.binTot, float64(r.binTot)/float64(r.hexTot), r.hdrChunk, r.ovfChunk, r.binBranch, r.zonedTot)
	}
	out += "\nzoneOnly = header chunks pruned away: the account proofs plus whatever the\n" +
		"code zone already carries. The proposal's witness is that plus one more\n" +
		"128-chunk code-zone group for the chunks it moves out of the header.\n"
	t.Log("witness cost of duplicated bytecode\n" + out)
}
