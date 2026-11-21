package execmodule_test

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// counterInit deploys a contract whose every call increments storage slot 0.
var counterInit = common.FromHex("69600054600101600055006000526000600a6016f3")

func (h *roundHarness) signed(key int, nonce uint64, to *common.Address, gas uint64, data []byte) []byte {
	h.t.Helper()
	k := h.keys[key]
	price := uint256.NewInt(h.m.Genesis.BaseFee().Uint64())
	var tx types.Transaction
	if to == nil {
		tx = types.NewContractCreation(nonce, uint256.NewInt(0), gas, price, data)
	} else {
		tx = types.NewTransaction(nonce, *to, uint256.NewInt(0), gas, price, data)
	}
	stx, err := types.SignTx(tx, *types.LatestSignerForChainID(h.m.ChainConfig.ChainID), k)
	require.NoError(h.t, err)
	var buf bytes.Buffer
	require.NoError(h.t, stx.MarshalBinary(&buf))
	return buf.Bytes()
}

// A block whose first round deploys contracts and whose later rounds each call one of them seals the root an
// independent node computes when it re-executes the block.
func TestRoundsAfterADeploySealTheRealRoot(t *testing.T) {
	for run := 1; run <= 5; run++ {
		h := newRoundHarness(t)
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)
		params, in := h.attrs(head)

		sender := crypto.PubkeyToAddress(h.keys[0].PublicKey)
		counter := types.CreateAddress(sender, 0)
		rounds := [][][]byte{{
			h.signed(0, 0, nil, 200_000, counterInit),
			h.signed(0, 1, &counter, 100_000, nil),
			h.signed(0, 2, nil, 200_000, counterInit),
		}}
		for n := uint64(3); n < 10; n++ {
			rounds = append(rounds, [][]byte{h.signed(0, n, &counter, 100_000, nil)})
		}
		txs := 0
		for i, r := range rounds {
			_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, r)
			require.NoError(t, err, "run %d round %d", run, i+1)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "run %d round %d: %s", run, i+1, vr.ValidationError)
			txs += len(r)
		}
		b := h.sealOnProducer(params, txs)
		require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, []*types.Block{b}),
			"run %d: an independent node re-executing the block must accept its root", run)
	}
}

// sealRounds pre-executes the rounds, seals the block and runs the producer's newPayload for it, but not its fork choice.
func (h *roundHarness) sealRounds(params *builder.Parameters, in execmodule.FlashblockInputs, rounds [][][]byte) *types.Block {
	h.t.Helper()
	txs := 0
	for i, r := range rounds {
		_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, r)
		require.NoError(h.t, err, "round %d", i+1)
		require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "round %d: %s", i+1, vr.ValidationError)
		txs += len(r)
	}
	br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
	require.NoError(h.t, err)
	require.Len(h.t, br.Block.Transactions(), txs)
	vr, err := validateChain(h.ctx, h.m.ExecModule, br.Block)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "newPayload: %s", vr.ValidationError)
	return br.Block
}

// The order a producer runs at the tip: block A sealed and its newPayload taken, then A's fork choice runs while the
// next block opens and takes its first rounds; B's later rounds follow, then its seal, newPayload and fork choice.
func TestRoundsBesideTheParentsForkChoiceSealTheRealRoot(t *testing.T) {
	roundsAfterParentForkChoice(t, true)
}

// The same with the parent's fork choice finished before the next block opens.
func TestRoundsAfterTheParentsForkChoiceSealTheRealRoot(t *testing.T) {
	roundsAfterParentForkChoice(t, false)
}

func roundsAfterParentForkChoice(t *testing.T, concurrent bool) {
	for run := 1; run <= 10; run++ {
		h := newRoundHarness(t)
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)
		sender := crypto.PubkeyToAddress(h.keys[0].PublicKey)
		counter := types.CreateAddress(sender, 0)

		paramsA, inA := h.attrs(head)
		a := h.sealRounds(paramsA, inA, [][][]byte{
			{h.signed(0, 0, nil, 200_000, counterInit)},
			{h.signed(0, 1, &counter, 100_000, nil), h.signed(1, 0, nil, 200_000, counterInit)},
		})

		paramsB, inB := h.attrs(a.Header())
		fcuA := make(chan error, 1)
		go func() {
			ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, a.Header())
			if err == nil && ur.Status != execmodule.ExecutionStatusSuccess {
				err = fmt.Errorf("fork choice to A: %v %s", ur.Status, ur.ValidationError)
			}
			fcuA <- err
		}()
		if !concurrent {
			require.NoError(t, <-fcuA)
			fcuA <- nil
		}
		roundsB := [][][]byte{{}, {
			h.signed(0, 2, nil, 200_000, counterInit),
			h.signed(0, 3, &counter, 100_000, nil),
			h.signed(0, 4, nil, 200_000, counterInit),
		}}
		for n := uint64(5); n < 12; n++ {
			roundsB = append(roundsB, [][]byte{h.signed(0, n, &counter, 100_000, nil)})
		}
		b := h.sealRounds(paramsB, inB, roundsB)
		require.NoError(t, <-fcuA, "run %d", run)
		ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, b.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, "run %d fork choice to B: %s", run, ur.ValidationError)

		require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, []*types.Block{a, b}),
			"run %d: an independent node re-executing the blocks must accept their roots", run)
	}
}

// The producer's steady state, block after block: each block's fork choice runs while the next block opens, and the
// next block's transactions then arrive over several rounds of different sizes — deploys and calls from two senders.
func TestAChainOfRoundBuiltBlocksSealsTheRealRoots(t *testing.T) {
	chainOfRoundBuiltBlocks(t, true)
}

// The same chain with each fork choice finished before the next block opens.
func TestAChainOfRoundBuiltBlocksAfterEachForkChoiceSealsTheRealRoots(t *testing.T) {
	chainOfRoundBuiltBlocks(t, false)
}

// The same chain with each block's transactions in one round.
func TestAChainOfOneRoundBlocksSealsTheRealRoots(t *testing.T) {
	oneRound = true
	defer func() { oneRound = false }()
	chainOfRoundBuiltBlocks(t, false)
}

var oneRound bool

func chainOfRoundBuiltBlocks(t *testing.T, overlap bool) {
	chainOfRoundBuiltBlocksWith(t, overlap, nil)
}

func chainOfRoundBuiltBlocksWith(t *testing.T, overlap bool, keys []string) {
	for run := 1; run <= 3; run++ {
		h := newRoundHarness(t)
		if keys != nil {
			h = newRoundHarnessWithKeys(t, [2]*ecdsa.PrivateKey{mustKey(t, keys[0]), mustKey(t, keys[1])})
		}
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)
		sender := crypto.PubkeyToAddress(h.keys[0].PublicKey)
		counter := types.CreateAddress(sender, 0)
		var nonce [2]uint64
		next := func(key int, to *common.Address, data []byte) []byte {
			gas := uint64(100_000)
			if to == nil {
				gas = 200_000
			}
			tx := h.signed(key, nonce[key], to, gas, data)
			nonce[key]++
			return tx
		}

		var chain []*types.Block
		fcu := make(chan error, 1)
		fcu <- nil
		for blk := 0; blk < 20; blk++ {
			params, in := h.attrs(head)
			_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, nil)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
			require.NoError(t, <-fcu, "run %d block %d: parent fork choice", run, blk+1)

			var rounds [][][]byte
			if blk == 0 {
				rounds = append(rounds, [][]byte{next(0, nil, counterInit)})
			}
			for r := 0; r < 1+blk%4; r++ {
				var round [][]byte
				for i := 0; i <= (blk+r)%3; i++ {
					if (blk+r+i)%5 == 4 {
						round = append(round, next(1, nil, counterInit))
					} else {
						round = append(round, next(i%2, &counter, nil))
					}
				}
				rounds = append(rounds, round)
			}
			if oneRound {
				var all [][]byte
				for _, r := range rounds {
					all = append(all, r...)
				}
				rounds = [][][]byte{all}
			}
			txs := 0
			for i, r := range rounds {
				_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, r)
				require.NoError(t, err, "run %d block %d round %d", run, blk+1, i+1)
				require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "run %d block %d round %d: %s", run, blk+1, i+1, vr.ValidationError)
				txs += len(r)
			}
			br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
			require.NoError(t, err)
			require.Len(t, br.Block.Transactions(), txs)
			vr, err = validateChain(h.ctx, h.m.ExecModule, br.Block)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "newPayload: %s", vr.ValidationError)
			b := br.Block
			chain = append(chain, b)
			head = b.Header()
			go func() {
				ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, b.Header())
				if err == nil && ur.Status != execmodule.ExecutionStatusSuccess {
					err = fmt.Errorf("%v %s", ur.Status, ur.ValidationError)
				}
				fcu <- err
			}()
			if !overlap {
				require.NoError(t, <-fcu)
				fcu <- nil
			}
		}
		require.NoError(t, <-fcu)
		if err := insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, chain); err != nil {
			t.Logf("FAILING KEYS %x %x: %v", crypto.FromECDSA(h.keys[0]), crypto.FromECDSA(h.keys[1]), err)
		}
		require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, chain),
			"run %d: an independent node re-executing the chain must accept every root", run)
	}
}

func mustKey(t *testing.T, hex string) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.HexToECDSA(hex)
	require.NoError(t, err)
	return k
}

var chainFailingKeys = []string{
	"86c0fc99521459df467ac0a26e1403d6af0489d6f4c346c442d26c7d9692e161",
	"97ebab0adf33463adbd6f2c4a6836f8b19552d8158ae7882a7585a4494bbac6d",
}

func TestAChainOfRoundBuiltBlocksSealsTheRealRootsForKnownFailingKeys(t *testing.T) {
	chainOfRoundBuiltBlocksWith(t, false, chainFailingKeys)
}

func TestAChainOfOneRoundBlocksSealsTheRealRootsForKnownFailingKeys(t *testing.T) {
	oneRound = true
	defer func() { oneRound = false }()
	chainOfRoundBuiltBlocksWith(t, false, chainFailingKeys)
}
