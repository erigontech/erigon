package execmoduletester

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/gointerfaces"
)

// Under EIP-8246 a contract that a tx creates over an existing account and destroys leaves the
// account with its balance and no storage. A slot that the contract wrote over an old value is zero
// after the tx, and the state-change stream announces it, so that a state cache drops the old value.
func TestStateChangesAnnounceASlotThatADestroyedContractWrote(t *testing.T) {
	key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{1}, 32))
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	initCode := program.New().Sstore(1, 7).Op(vm.ADDRESS, vm.SELFDESTRUCT).Bytes()
	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
	slot := common.HexToHash("0x01")
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
		factory: {Code: program.New().Create2(initCode, 0).Op(vm.STOP).Bytes()},
		x:       {Balance: big.NewInt(5), Storage: map[common.Hash]common.Hash{slot: common.HexToHash("0x09")}},
	}}
	m := New(t, WithGenesisSpec(gspec), WithKey(key))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), factory, uint256.NewInt(0), 1_000_000, uint256.NewInt(10_000_000_000), nil), *types.LatestSignerForChainID(config.ChainID), key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	wait := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), pack.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	batch := wait(pack.TopBlock.Hash())

	announced := false
	for _, cb := range batch.ChangeBatch {
		for _, ch := range cb.Changes {
			if gointerfaces.ConvertH160toAddress(ch.Address) != x {
				continue
			}
			for _, sc := range ch.StorageChanges {
				if gointerfaces.ConvertH256ToHash(sc.Location) == slot {
					announced = true
					require.Empty(t, sc.Data)
				}
			}
		}
	}
	require.True(t, announced, "the stream announces the slot")
}
