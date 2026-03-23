package l1sync

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	arbBlocks "github.com/erigontech/erigon/arb/blocks"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/nitro-erigon/arbos"
	"github.com/erigontech/nitro-erigon/arbos/arbosState"
	"github.com/erigontech/nitro-erigon/arbos/l2pricing"
)

// canExecute returns true if the service has the temporal DB and chain config
// needed for EVM execution (i.e. running integrated with erigon, not standalone).
func (s *L1SyncService) canExecute() bool {
	return s.temporalDB != nil && s.chainConfig != nil
}

// executeBlock runs every transaction in the block through the EVM with full
// Arbitrum execution: ProcessingHook, redeems, ArbOS state refresh, and
// FinalizeBlock. This mirrors ProduceBlockAdvanced but uses erigon's
// SharedDomains for state reads/writes.
func (s *L1SyncService) executeBlock(block *types.Block, temporalTx kv.TemporalRwTx, delayedMessagesRead uint64) error {
	// 1. SharedDomains setup (same as before)
	doms, err := dbstate.NewSharedDomains(temporalTx, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create shared domains: %w", err)
	}
	defer doms.Close()

	reader := state.NewReaderV3(doms.AsGetter(temporalTx))
	txNum := doms.TxNum()
	writer := state.NewWriter(doms.AsPutDel(temporalTx), nil, txNum)
	ibs := state.New(reader)

	// 2. Arbitrum state setup
	ibsArb := state.NewArbitrum(ibs)
	header := block.Header()
	doms.SetBlockNum(header.Number.Uint64())

	// 3. BLOCKHASH function using header cache
	blockHashFn := core.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
		if h, ok := s.headerCache[number]; ok {
			return h, nil
		}
		return nil, fmt.Errorf("header %d not in cache", number)
	})

	// 4. Create EVM with block context
	blockContext := core.NewEVMBlockContext(header, blockHashFn, arbos.Engine{}, &header.Coinbase, s.chainConfig)
	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, s.chainConfig, vm.Config{})

	// 5. Set Arbitrum ProcessingHook (mirrors exec3 state.go:234-239)
	emptyMsg := &types.Message{}
	evm.ProcessingHook = arbos.NewTxProcessorIBS(evm, ibsArb, emptyMsg)

	// 6. Gas pool (matches ProduceBlockAdvanced)
	gp := new(core.GasPool).AddGas(l2pricing.GethBlockGasLimit)

	// 7. Tx loop with redeems (mirrors block_processor.go:245-498)
	txes := block.Transactions()
	redeems := types.Transactions{}
	complete := types.Transactions{}

	for len(txes) > 0 || len(redeems) > 0 {
		var tx types.Transaction
		if len(redeems) > 0 {
			tx = redeems[0]
			redeems = redeems[1:]
		} else {
			tx = txes[0]
			txes = txes[1:]
		}

		txNum++
		doms.SetTxNum(txNum)
		writer.SetTxNum(txNum)

		// Create message and set on ProcessingHook before execution
		signer := types.MakeSigner(s.chainConfig, header.Number.Uint64(), header.Time)
		msg, err := tx.AsMessage(*signer, header.BaseFee, evm.ChainRules())
		if err != nil {
			return fmt.Errorf("tx AsMessage failed in block %d: %w", header.Number.Uint64(), err)
		}
		evm.ProcessingHook.SetMessage(msg, ibsArb)

		ibs.SetTxContext(header.Number.Uint64(), len(complete))

		receipt, result, err := core.ApplyArbTransactionVmenv(
			s.chainConfig, arbos.Engine{}, gp, ibsArb, writer,
			header, tx, &header.GasUsed, header.BlobGasUsed, vm.Config{}, evm,
		)

		if err != nil {
			if tx.Type() == types.ArbitrumInternalTxType {
				return fmt.Errorf("internal tx failed in block %d: %w", header.Number.Uint64(), err)
			}
			// Skip invalid user txs (same as ProduceBlockAdvanced:388-403)
			s.logger.Debug("tx error, skipping", "block", header.Number, "err", err)
			continue
		}

		// Internal tx: refresh ArbOS state + update header, then check error
		// (matches ProduceBlockAdvanced ordering: block_processor.go:406-420)
		if tx.Type() == types.ArbitrumInternalTxType {
			// ArbOS might have upgraded, refresh state first
			extraInfo := arbBlocks.DeserializeHeaderExtraInformation(header)
			osState, err := arbosState.OpenSystemArbosState(ibsArb, nil, true)
			if err != nil {
				return fmt.Errorf("failed to open ArbOS state after internal tx in block %d: %w", header.Number.Uint64(), err)
			}
			extraInfo.ArbOSFormatVersion = osState.ArbOSVersion()
			extraInfo.UpdateHeaderWithInfo(header)

			if result.Err != nil {
				return fmt.Errorf("internal tx execution error in block %d: %w", header.Number.Uint64(), result.Err)
			}
		}

		// Collect redeems (block_processor.go:463)
		redeems = append(redeems, result.ScheduledTxes...)
		complete = append(complete, tx)

		s.logger.Debug("tx executed", "block", header.Number, "txIdx", len(complete)-1,
			"gasUsed", receipt.GasUsed, "status", receipt.Status)
	}

	// 8. Set header nonce to delayedMessagesRead (block_processor.go:510)
	binary.BigEndian.PutUint64(header.Nonce[:], delayedMessagesRead)

	// 9. Finalize block (block_processor.go:512)
	arbos.FinalizeBlock(header, complete, ibsArb, s.chainConfig)

	// 10. Flush state
	return doms.Flush(s.ctx, temporalTx)
}
