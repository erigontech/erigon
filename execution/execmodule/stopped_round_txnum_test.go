package execmodule_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

type burnChain struct {
	t          *testing.T
	m          *execmoduletester.ExecModuleTester
	key        *ecdsa.PrivateKey
	burner     common.Address
	recipient  common.Address
	beaconRoot common.Hash
}

func newBurnChain(t *testing.T) *burnChain {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	recipientKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	c := &burnChain{t: t, key: key, burner: common.HexToAddress("0x00000000000000000000000000000000000b0817"),
		recipient: crypto.PubkeyToAddress(recipientKey.PublicKey)}
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config:   &cfg,
		GasLimit: 300_000_000,
		Alloc: types.GenesisAlloc{
			crypto.PubkeyToAddress(key.PublicKey): {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			c.burner:                              {Balance: new(big.Int), Code: burnerCode},
		},
	}
	c.m = execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key),
		execmoduletester.WithTxnIDsFollowParent())
	return c
}

func (c *burnChain) attrs(parent *types.Header) (*builder.Parameters, execmodule.FlashblockInputs) {
	params := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time + 1,
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &c.beaconRoot,
	}
	return params, execmodule.FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(c.m.ChainConfig, parent),
		Timestamp:             params.Timestamp,
		ParentBeaconBlockRoot: c.beaconRoot,
		Withdrawals:           params.Withdrawals,
	}
}

func (c *burnChain) burn(nonce uint64) []byte {
	tx, err := types.SignTx(
		types.NewTransaction(nonce, c.burner, uint256.NewInt(0), 10_000_000, uint256.NewInt(c.m.Genesis.BaseFee().Uint64()), nil),
		*types.LatestSignerForChainID(c.m.ChainConfig.ChainID), c.key)
	require.NoError(c.t, err)
	var buf bytes.Buffer
	require.NoError(c.t, tx.MarshalBinary(&buf))
	return buf.Bytes()
}

func (c *burnChain) round(ctx context.Context, in execmodule.FlashblockInputs, txs [][]byte) (execmodule.ValidationResult, error) {
	_, _, vr, err := c.m.ExecModule.PreExecuteFlashblock(ctx, in, txs)
	return vr, err
}

func (c *burnChain) sealAndValidate(params *builder.Parameters) *types.Block {
	br, err := c.m.ExecModule.SealBlock(c.t.Context(), params, false)
	require.NoError(c.t, err)
	vr, err := validateChain(c.t.Context(), c.m.ExecModule, br.Block)
	require.NoError(c.t, err)
	require.Equal(c.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	return br.Block
}

func (c *burnChain) requireSpans(blocks ...*types.Block) {
	ctx := c.t.Context()
	for _, b := range blocks {
		ur, err := forkChoiceTo(ctx, c.m.ExecModule, b.Header())
		require.NoError(c.t, err)
		require.Equal(c.t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
	}
	require.NoError(c.t, c.m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		for _, b := range blocks {
			first, err := c.m.BlockReader.TxnumReader().Min(ctx, tx, b.NumberU64())
			require.NoError(c.t, err)
			last, err := c.m.BlockReader.TxnumReader().Max(ctx, tx, b.NumberU64())
			require.NoError(c.t, err)
			require.Equal(c.t, uint64(len(b.Transactions())+2), last-first+1,
				"block %d spans %d txNums for %d transactions", b.NumberU64(), last-first+1, len(b.Transactions()))
		}
		return nil
	}))
}

func (c *burnChain) perBurn(ctx context.Context, in execmodule.FlashblockInputs) time.Duration {
	start := time.Now()
	vr, err := c.round(ctx, in, [][]byte{c.burn(0)})
	require.NoError(c.t, err)
	require.Equal(c.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	return time.Since(start)
}

// A round cut short keeps a prefix; the block sealed right after it, and the block opened on it before the fork
// choice, each span exactly their own transactions in the txNum index.
func TestABlockSealedAfterAStoppedRoundSpansOnlyWhatItKept(t *testing.T) {
	c := newBurnChain(t)
	ctx := t.Context()
	params, in := c.attrs(c.m.Genesis.HeaderNoCopy())
	perBurn := c.perBurn(ctx, in)

	const burns = 24
	round := make([][]byte, 0, burns)
	for i := uint64(1); i <= burns; i++ {
		round = append(round, c.burn(i))
	}
	deadlined, cancel := context.WithDeadline(ctx, time.Now().Add(perBurn*2))
	defer cancel()
	_, err := c.round(deadlined, in, round)
	var stopped execmodule.ExecutionStopped
	require.ErrorAs(t, err, &stopped)
	require.Positive(t, stopped.Applied)
	require.Less(t, stopped.Applied, burns)

	sealed := c.sealAndValidate(params)
	require.Len(t, sealed.Transactions(), 1+stopped.Applied)
	paramsB, inB := c.attrs(sealed.Header())
	vr, err := c.round(ctx, inB, [][]byte{c.burn(uint64(1 + stopped.Applied))})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	c.requireSpans(sealed, c.sealAndValidate(paramsB))
}

// A round that executes and then loses its commit is rolled back out of the body; the block's txNum span has to follow
// that body whether the block is next re-stamped with new withdrawals or sealed as it stands.
func TestABlockAfterARoundThatLostItsCommitSpansItsBody(t *testing.T) {
	for _, restamp := range []bool{false, true} {
		c := newBurnChain(t)
		ctx := t.Context()
		params, in := c.attrs(c.m.Genesis.HeaderNoCopy())
		c.perBurn(ctx, in)

		lost := execmodule.WithRoundCommit(ctx, func() bool { return false })
		_, err := c.round(lost, in, [][]byte{c.burn(1)})
		var stopped execmodule.ExecutionStopped
		require.ErrorAs(t, err, &stopped, "restamp=%v", restamp)

		if restamp {
			params.Withdrawals = []*types.Withdrawal{{Index: 1, Validator: 1, Address: c.recipient, Amount: 1}}
		}
		sealed := c.sealAndValidate(params)
		require.Len(t, sealed.Transactions(), 1, "restamp=%v", restamp)
		paramsB, inB := c.attrs(sealed.Header())
		vr, err := c.round(ctx, inB, [][]byte{c.burn(1)})
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		c.requireSpans(sealed, c.sealAndValidate(paramsB))
	}
}
