package executiontests

import (
	"crypto/ecdsa"
	"encoding/binary"
	"math/big"
	"os"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Validates one Amsterdam block of transfers, independent storage writes and
// storage writes that read the previous tx's slot. With NEWPAYLOAD_PHASES=true it
// logs the [np-phase] end timeline of the validation.
func TestNpEndProbeMixedDependency(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Cleanup(statecfg.SetCommitmentV3(true))
	n := 3000
	if v, err := strconv.Atoi(os.Getenv("NP_END_PROBE_TXS")); err == nil && v > 0 {
		n = v
	}
	store := common.HexToAddress("0x5707e")
	code := program.New().Push(0).Op(vm.CALLDATALOAD, vm.SLOAD).Push(1).Op(vm.ADD).Push(32).Op(vm.CALLDATALOAD, vm.SSTORE, vm.STOP)
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	alloc := types.GenesisAlloc{store: {Code: code.Bytes()}}
	keys := make([]*ecdsa.PrivateKey, n)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes(binary.BigEndian.AppendUint32(nil, uint32(i+1)), 32))
		require.NoError(t, err)
		keys[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}
	gspec := &types.Genesis{Config: config, GasLimit: 300_000_000, Alloc: alloc}
	signer := types.LatestSignerForChainID(config.ChainID)
	slot := func(i int) []byte { return common.LeftPadBytes(binary.BigEndian.AppendUint64(nil, uint64(i)), 32) }

	gen := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := gen.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for i, key := range keys {
			to, data := common.BigToAddress(big.NewInt(int64(0x100000+i))), []byte(nil)
			switch i % 3 {
			case 1:
				to, data = store, append(slot(i), slot(i)...)
			case 2:
				to, data = store, append(slot(i-1), slot(i)...)
			}
			txn, err := types.SignTx(types.NewTransaction(0, to, uint256.NewInt(1), 100_000, uint256.NewInt(10_000_000_000), data), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.Len(t, chainPack.TopBlock.Transactions(), n)

	prevWorkers := ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = prevWorkers })
	ethconfig.Defaults.Sync.ExecWorkerCount = 8
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]), execmoduletester.WithExperimentalBAL())
	status, err := m.InsertBlocks(t.Context(), chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	stagedsync.NpEndStart()
	result, err := m.ValidateChain(t.Context(), chainPack.TopBlock)
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	stagedsync.NpEndLog(logger, chainPack.TopBlock.NumberU64())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus, result.ValidationError)
}

// Validates one Amsterdam block of zero-value transfers from distinct senders to
// distinct empty accounts. With NEWPAYLOAD_PHASES=true it logs the [np-phase] end
// timeline of the validation.
func TestNpEndProbeEtherTransfers(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Cleanup(statecfg.SetCommitmentV3(true))
	n := 20000
	if v, err := strconv.Atoi(os.Getenv("NP_END_PROBE_TXS")); err == nil && v > 0 {
		n = v
	}
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	alloc := types.GenesisAlloc{}
	keys := make([]*ecdsa.PrivateKey, n)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes(binary.BigEndian.AppendUint32(nil, uint32(i+1)), 32))
		require.NoError(t, err)
		keys[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}
	gspec := &types.Genesis{Config: config, GasLimit: 2_000_000_000, Alloc: alloc}
	signer := types.LatestSignerForChainID(config.ChainID)

	gen := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := gen.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for i, key := range keys {
			to := common.BigToAddress(big.NewInt(int64(0x100000 + i)))
			txn, err := types.SignTx(types.NewTransaction(0, to, uint256.NewInt(0), 50_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.Len(t, chainPack.TopBlock.Transactions(), n)
	for _, r := range chainPack.Receipts[0] {
		require.Equal(t, types.ReceiptStatusSuccessful, r.Status)
	}

	prevWorkers := ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = prevWorkers })
	ethconfig.Defaults.Sync.ExecWorkerCount = 8
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]), execmoduletester.WithExperimentalBAL())
	status, err := m.InsertBlocks(t.Context(), chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	stagedsync.NpEndStart()
	result, err := m.ValidateChain(t.Context(), chainPack.TopBlock)
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	stagedsync.NpEndLog(logger, chainPack.TopBlock.NumberU64())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus, result.ValidationError)
}
