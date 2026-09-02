package jsonrpc

// A 1,000-slot contract read at three depths, hex against bin.
//
// Storage layout, not slot count, is what moves a binary witness: slots below 64
// sit in the account header under 34-byte keys sharing the account's stem, while
// everything above lands in the storage zone under 66-byte keys, one stem per
// 256-slot group. Mapping slots are keccak images, so they scatter one per group.
//
// The contract SLOADs a countdown of slots so one transaction touches N of them.

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

const pbinWhaleSlots = 1000

// pbinWhaleReader builds runtime that reads `n` slots. Sequential walks slot
// numbers directly; mapping hashes each index first, which is what puts every
// slot in its own storage-zone group.
func pbinWhaleReader(n int, mapping bool) []byte {
	code := []byte{0x61, byte(n >> 8), byte(n), 0x5b, 0x80} // PUSH2 n; JUMPDEST; DUP1
	if mapping {
		code = append(code,
			0x60, 0x00, 0x52, // PUSH1 0; MSTORE  -> mem[0] = i
			0x60, 0x20, 0x60, 0x00, 0x20, // PUSH1 32; PUSH1 0; SHA3 -> keccak(i)
		)
	}
	return append(code,
		0x54, 0x50, // SLOAD; POP
		0x60, 0x01, 0x90, 0x03, // PUSH1 1; SWAP1; SUB  -> i-1
		0x80, 0x60, 0x03, 0x57, // DUP1; PUSH1 3; JUMPI
		0x00, // STOP
	)
}

func pbinWhaleSlotKey(i int, mapping bool) common.Hash {
	if !mapping {
		return common.BigToHash(big.NewInt(int64(i)))
	}
	var buf [32]byte
	big.NewInt(int64(i)).FillBytes(buf[:])
	return crypto.Keccak256Hash(buf[:])
}

type pbinWhaleRow struct {
	layout                       string
	touched                      int
	hexNodes, hexState, hexTotal int
	binNodes, binTotal           int
	binLeaf, binBranch           int
	binHdr, binZone              int
}

var pbinWhaleCases = []struct {
	layout  string
	mapping bool
	touch   int
}{
	{"sequential", false, 8},
	{"sequential", false, 64},
	{"sequential", false, pbinWhaleSlots},
	{"mapping", true, 8},
	{"mapping", true, 64},
	{"mapping", true, pbinWhaleSlots},
}

// pbinWhaleChain allocates both contracts with 1,000 slots at genesis, then reads
// each depth in its own block so every measured witness is a pure read.
func pbinWhaleChain(t *testing.T) *pbinWitnessChain {
	t.Helper()
	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bank := crypto.PubkeyToAddress(bankKey.PublicKey)
	funds, ok := new(big.Int).SetString("100000000000000000000", 10)
	require.True(t, ok)

	alloc := types.GenesisAlloc{bank: {Balance: funds}}
	addrs := map[bool]map[int]common.Address{false: {}, true: {}}
	for _, mapping := range []bool{false, true} {
		storage := make(map[common.Hash]common.Hash, pbinWhaleSlots)
		for i := 1; i <= pbinWhaleSlots; i++ {
			storage[pbinWhaleSlotKey(i, mapping)] = common.BigToHash(big.NewInt(int64(i)))
		}
		for _, c := range pbinWhaleCases {
			if c.mapping != mapping {
				continue
			}
			a := common.BigToAddress(big.NewInt(int64(0x9000 + len(alloc))))
			alloc[a] = types.GenesisAccount{
				Balance: big.NewInt(1),
				Code:    pbinWhaleReader(c.touch, mapping),
				Storage: storage,
			}
			addrs[mapping][c.touch] = a
		}
	}

	chainConfig := new(chain.Config)
	require.NoError(t, copier.CopyWithOption(chainConfig, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chainConfig, Alloc: alloc, GasLimit: 60_000_000,
		}),
		execmoduletester.WithKey(bankKey))

	signer := types.LatestSignerForChainID(nil)
	gasPrice := uint256.NewInt(1_000_000_000)
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, len(pbinWhaleCases),
		func(i int, b *blockgen.BlockGen) {
			c := pbinWhaleCases[i]
			to := addrs[c.mapping][c.touch]
			txn := &types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: b.TxNonce(bank), To: &to, GasLimit: 30_000_000,
			}}
			txn.GasPrice = *gasPrice
			signed, err := types.SignTx(txn, *signer, bankKey)
			require.NoError(t, err)
			b.AddTx(signed)
		})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	return &pbinWitnessChain{m: m, pack: pack}
}

func TestPBinWhaleWitness(t *testing.T) {
	withCommitmentHistory(t)
	rows := make([]pbinWhaleRow, len(pbinWhaleCases))
	for i, c := range pbinWhaleCases {
		rows[i].layout, rows[i].touched = c.layout, c.touch
	}

	t.Run("hex", func(t *testing.T) {
		c := pbinWhaleChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i := range pbinWhaleCases {
			w := pbinWitnessOf(t, api, uint64(i+1))
			rows[i].hexNodes = len(w.State)
			rows[i].hexState = sumBytes(w.State)
			rows[i].hexTotal = rows[i].hexState + sumBytes(w.Codes) + sumBytes(w.Headers)
		}
	})

	t.Run("bin", func(t *testing.T) {
		withBinCommitmentDatadir(t)
		c := pbinWhaleChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i := range pbinWhaleCases {
			w := pbinWitnessOf(t, api, uint64(i+1))
			r := &rows[i]
			r.binNodes = len(w.State)
			for _, n := range w.State {
				r.binTotal += len(n)
				k := pbinLeafKeyOf(n)
				if k == nil {
					r.binBranch += len(n)
					continue
				}
				r.binLeaf += len(n)
				switch sub := k[len(k)-1]; {
				case k[0] == 0xFF:
					r.binZone++
				case k[0] == 0x00 && sub >= 64 && sub < 128:
					r.binHdr++
				}
			}
			r.binTotal += sumBytes(w.Headers)
		}
		// The property the table exists to show: a slot's number, not its count,
		// decides the zone. Slots under 64 sit in the account's header window;
		// everything else, and every keccak-mapped slot, gets its own storage-zone
		// group.
		for i, c := range pbinWhaleCases {
			r := &rows[i]
			if c.mapping {
				require.Zero(t, r.binHdr, "%s/%d: a mapped slot cannot reach the header window", c.layout, c.touch)
				require.NotZero(t, r.binZone, "%s/%d: mapped slots must land in the storage zone", c.layout, c.touch)
				continue
			}
			require.NotZero(t, r.binHdr, "%s/%d: slots under 64 must land in the header window", c.layout, c.touch)
			if c.touch < 64 {
				require.Zero(t, r.binZone, "%s/%d: no slot reaches the storage zone", c.layout, c.touch)
			} else {
				require.NotZero(t, r.binZone, "%s/%d: slots from 64 up must land in the storage zone", c.layout, c.touch)
			}
		}
	})

	out := fmt.Sprintf("%d slots stored; one block per read depth\n", pbinWhaleSlots)
	out += fmt.Sprintf("%-11s %6s | %7s %9s | %7s %9s %8s | %9s %9s %6s %6s\n",
		"layout", "touch", "hexNod", "hex tot", "binNod", "bin tot", "bin/hex", "binLeafB", "binBrB", "hdr", "zone")
	for _, r := range rows {
		out += fmt.Sprintf("%-11s %6d | %7d %9d | %7d %9d %7.2fx | %9d %9d %6d %6d\n",
			r.layout, r.touched, r.hexNodes, r.hexTotal, r.binNodes, r.binTotal,
			float64(r.binTotal)/float64(max(r.hexTotal, 1)), r.binLeaf, r.binBranch, r.binHdr, r.binZone)
	}
	t.Log(out)
}
