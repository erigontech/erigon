package protocol

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func newArbTestStateAndConfig(t *testing.T) (*state.IntraBlockState, *chain.Config, *ecdsa.PrivateKey) {
	t.Helper()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))

	s.CreateAccount(sender, true)
	s.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	s.SetNonce(sender, 0)

	cfg := &chain.Config{
		ChainID:               big.NewInt(1),
		HomesteadBlock:        new(big.Int),
		TangerineWhistleBlock: new(big.Int),
		SpuriousDragonBlock:   new(big.Int),
		ByzantiumBlock:        new(big.Int),
		ConstantinopleBlock:   new(big.Int),
		PetersburgBlock:       new(big.Int),
		IstanbulBlock:         new(big.Int),
		BerlinBlock:           new(big.Int),
		LondonBlock:           new(big.Int),
	}

	return s, cfg, key
}

func newArbTestBlockCtx(coinbase accounts.Address) evmtypes.BlockContext {
	return evmtypes.BlockContext{
		BlockNumber: 1,
		GasLimit:    30_000_000,
		BaseFee:     *uint256.NewInt(1),
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
			bal, err := ibs.GetBalance(addr)
			if err != nil {
				return false, err
			}
			return bal.Cmp(&val) >= 0, nil
		},
		Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
			if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
		},
	}
}

func TestApplyArbTransaction_MsgTxIsSet(t *testing.T) {
	s, cfg, key := newArbTestStateAndConfig(t)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	recipient := common.HexToAddress("0xbbbb")
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))

	header := &types.Header{
		Number:   big.NewInt(1),
		GasLimit: 30_000_000,
		BaseFee:  big.NewInt(1),
		Time:     1000,
	}

	signer := *types.MakeSigner(cfg, 1, header.Time)
	txn, err := types.SignTx(types.NewTransaction(0, recipient, uint256.NewInt(0), 50000, uint256.NewInt(1_000_000_000), nil), signer, key)
	require.NoError(t, err)

	evmInst := vm.NewEVM(newArbTestBlockCtx(coinbase), evmtypes.TxContext{Origin: sender}, s, cfg, vm.Config{})

	var capturedMsg *types.Message
	oldReadyEVM := vm.ReadyEVMForL2
	vm.ReadyEVMForL2 = func(evm *vm.EVM, msg *types.Message) {
		capturedMsg = msg
	}
	defer func() { vm.ReadyEVMForL2 = oldReadyEVM }()

	usedGas := uint64(0)
	gp := new(GasPool).AddGas(30_000_000)
	w := state.NewNoopWriter()
	engine := ethash.NewFaker()

	_, _, err = applyArbTransaction(cfg, engine, gp, s, w, header, txn, &usedGas, nil, evmInst, vm.Config{})
	require.NoError(t, err)
	require.NotNil(t, capturedMsg, "ReadyEVMForL2 should have been called")
	require.NotNil(t, capturedMsg.Tx, "msg.Tx must be set for poster gas calculation")
}

func TestProcessParentBlockHash_ConstantsValid(t *testing.T) {
	// Verify EIP-2935 constants are properly defined.
	require.False(t, params.SystemAddress.IsNil())
	require.False(t, params.HistoryStorageAddress.IsNil())

	// Verify ProcessParentBlockHash does not panic with a valid EVM.
	s, cfg, _ := newArbTestStateAndConfig(t)
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))
	evmInst := vm.NewEVM(newArbTestBlockCtx(coinbase), evmtypes.TxContext{}, s, cfg, vm.Config{})
	prevHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

	ProcessParentBlockHash(prevHash, evmInst)
}
