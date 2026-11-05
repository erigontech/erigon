package executiontests

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	libchain "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Probe: BAL blocks of transfers, deploys and storage writes with a tiny step size, so each block crosses
// several mid-block step edges; the parallel executor must reproduce the serial roots.
func TestProbeParallelStepEdgeFuzz(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })

	counter := []byte{byte(vm.PUSH1), 0, byte(vm.SLOAD), byte(vm.PUSH1), 1, byte(vm.ADD), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP)}
	deployCode := func(runtime []byte) []byte {
		n := byte(len(runtime))
		return append([]byte{byte(vm.PUSH1), n, byte(vm.PUSH1), 12, byte(vm.PUSH1), 0, byte(vm.CODECOPY), byte(vm.PUSH1), n, byte(vm.PUSH1), 0, byte(vm.RETURN)}, runtime...)
	}
	config := libchain.AllProtocolChanges
	gasPrice := uint256.NewInt(10_000_000_000)
	for seed := range 16 {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 3125))
			keys := make([]*ecdsa.PrivateKey, 8)
			alloc := types.GenesisAlloc{}
			for i := range keys {
				var kb [32]byte
				for j := range kb {
					kb[j] = byte(rng.IntN(256))
				}
				kb[0] |= 1
				key, err := crypto.ToECDSA(kb[:])
				require.NoError(t, err)
				keys[i] = key
				alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(common.Ether)}
			}
			dbg.Exec3Parallel = false
			ethconfig.Defaults.Sync.ExecWorkerCount = 4
			m := execmoduletester.New(t, execmoduletester.WithKey(keys[0]), execmoduletester.WithGenesisSpec(&types.Genesis{Config: config, Alloc: alloc, GasLimit: 60_000_000}),
				execmoduletester.WithExperimentalBAL(), execmoduletester.WithStepSize(8))
			signer := types.LatestSignerForChainID(config.ChainID)
			nonces := make([]uint64, len(keys))
			var deployed []common.Address
			chainPack, err := m.GenerateChain(4, func(_ int, b *blockgen.BlockGen) {
				for range 40 {
					s := rng.IntN(len(keys))
					from := crypto.PubkeyToAddress(keys[s].PublicKey)
					var txn types.Transaction
					switch r := rng.IntN(4); {
					case r == 0 || len(deployed) == 0:
						deployed = append(deployed, types.CreateAddress(from, nonces[s]))
						txn = types.NewContractCreation(nonces[s], uint256.NewInt(0), 500_000, gasPrice, deployCode(counter))
					case r == 1:
						txn = types.NewTransaction(nonces[s], deployed[rng.IntN(len(deployed))], uint256.NewInt(0), 300_000, gasPrice, nil)
					default:
						to := crypto.PubkeyToAddress(keys[rng.IntN(len(keys))].PublicKey)
						if rng.IntN(2) == 0 {
							to = common.BytesToAddress([]byte{byte(rng.IntN(200)) + 1, 0xee})
						}
						txn = types.NewTransaction(nonces[s], to, uint256.NewInt(uint64(rng.IntN(3))), 300_000, gasPrice, nil)
					}
					signed, err := types.SignTx(txn, *signer, keys[s])
					require.NoError(t, err)
					b.AddTx(signed)
					nonces[s]++
				}
			})
			require.NoError(t, err)
			for i := range chainPack.Blocks {
				for _, r := range chainPack.Receipts[i] {
					require.Equal(t, types.ReceiptStatusSuccessful, r.Status)
				}
			}
			dbg.Exec3Parallel = true
			require.NoError(t, m.InsertChain(chainPack))
		})
	}
}
