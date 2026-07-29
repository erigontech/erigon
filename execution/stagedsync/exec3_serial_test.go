package stagedsync

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// newSerialResumeTestExec wires a serialExecutor with a real worker over db.
func newSerialResumeTestExec(t *testing.T, db kv.TemporalRwDB, config *chain.Config, engine rules.Engine) *serialExecutor {
	logger := log.New()
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	t.Cleanup(roTx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), roTx, logger)
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	se := &serialExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: config,
				db:          db,
				engine:      engine,
				vmConfig:    &vm.Config{},
			},
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, false, logger)),
			logger: logger,
		},
	}
	require.NoError(t, se.resetWorkers(context.Background(), se.rs, roTx))
	return se
}

// TestSerialResumeValidatesRestoredBlockGas pins the serial resumed-block path:
// the suffix executes against latest state, the prefix replays from history, and
// post-execution validation checks full-block gas and receipts. A header carrying
// only the suffix gas must be rejected — proving validation runs with restored
// totals rather than batch-local ones.
func TestSerialResumeValidatesRestoredBlockGas(t *testing.T) {
	config := chain.TestChainBerlinConfig

	run := func(t *testing.T, headerGasUsed uint64) error {
		db := newResumeTestDB(t)
		engine := ethash.NewFaker()

		tx0 := signSelfSendTx(t, 0, 1, 1, 21000, config, 0)
		tx1 := signSelfSendTx(t, 1, 1, 1, 21000, config, 0)
		txs := types.Transactions{tx0, tx1}

		expectedReceipts := types.Receipts{
			{Type: types.LegacyTxType, Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: 21000},
			{Type: types.LegacyTxType, Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: 42000},
		}
		header := &types.Header{
			Number:      *uint256.NewInt(1),
			GasLimit:    10_000_000,
			GasUsed:     headerGasUsed,
			ReceiptHash: types.DeriveSha(expectedReceipts),
			Bloom:       types.CreateBloom(expectedReceipts),
		}

		// The prefix tx0 ran in an earlier batch: seed its post-state as latest
		// (pre-state as history) plus its receipt-domain row.
		seedResumeTestDB(t, db, func(putter kv.TemporalPutDel) error {
			preState := accounts.NewAccount()
			preState.Balance = *uint256.NewInt(1_000_000_000)
			preStateEnc := accounts.SerialiseV3(&preState)
			if err := putter.DomainPut(kv.AccountsDomain, senderIsCoinbaseKey.rawAddress[:], preStateEnc, 0, nil); err != nil {
				return err
			}
			postState := accounts.NewAccount()
			postState.Nonce = 1
			postState.Balance = *uint256.NewInt(1_000_000_000 - 21000)
			if err := putter.DomainPut(kv.AccountsDomain, senderIsCoinbaseKey.rawAddress[:], accounts.SerialiseV3(&postState), 1, preStateEnc); err != nil {
				return err
			}
			return rawtemporaldb.AppendReceipt(putter, 0, 21000, 0, 1)
		})

		se := newSerialResumeTestExec(t, db, config, engine)

		suffixTask := &exec.TxTask{
			Header:  header,
			TxNum:   2,
			TxIndex: 1,
			Config:  config,
			Txs:     txs,
			EvmBlockContext: protocol.NewEVMBlockContext(
				header, protocol.GetHashFn(header, nil), engine, accounts.NilAddress, config),
		}
		blockEndTask := &exec.TxTask{
			Header:  header,
			TxNum:   3,
			TxIndex: 2,
			Config:  config,
			Txs:     txs,
		}

		_, err := se.executeBlock(context.Background(), []exec.Task{suffixTask, blockEndTask}, true, false)
		return err
	}

	t.Run("restored gas matches header", func(t *testing.T) {
		require.NoError(t, run(t, 42000))
	})

	t.Run("suffix-only gas in header is rejected", func(t *testing.T) {
		err := run(t, 21000)
		require.ErrorIs(t, err, rules.ErrInvalidBlock)
		require.ErrorContains(t, err, "gas used by execution: 42000")
	})
}
