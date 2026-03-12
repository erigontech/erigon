package l1sync

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/nitro-erigon/arbos"
)

// canExecute returns true if the service has the temporal DB and chain config
// needed for EVM execution (i.e. running integrated with erigon, not standalone).
func (s *L1SyncService) canExecute() bool {
	return s.temporalDB != nil && s.chainConfig != nil
}

// executeBlock runs every transaction in the block through the EVM using
// SharedDomains for state reads/writes. The caller provides the temporal tx
// and is responsible for committing it. This avoids opening a second write
// transaction which would deadlock MDBX.
func (s *L1SyncService) executeBlock(block *types.Block, temporalTx kv.TemporalRwTx) error {
	doms, err := dbstate.NewSharedDomains(temporalTx, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create shared domains: %w", err)
	}
	defer doms.Close()

	reader := state.NewReaderV3(doms.AsGetter(temporalTx))
	txNum := doms.TxNum() // start from current position
	writer := state.NewWriter(doms.AsPutDel(temporalTx), nil, txNum)
	ibs := state.New(reader)

	header := block.Header()
	gp := new(core.GasPool).AddGas(header.GasLimit)
	doms.SetBlockNum(header.Number.Uint64())

	// TODO: implement proper BLOCKHASH lookup from DB
	blockHashFn := func(n uint64) (common.Hash, error) {
		return common.Hash{}, nil
	}

	var gasUsed uint64
	for i, txn := range block.Transactions() {
		txNum++
		doms.SetTxNum(txNum)
		writer.SetTxNum(txNum)

		ibs.SetTxContext(header.Number.Uint64(), i)

		receipt, _, err := core.ApplyTransaction(s.chainConfig, blockHashFn, arbos.Engine{}, &header.Coinbase, gp, ibs, writer, header, txn, &gasUsed, nil, vm.Config{})
		if err != nil {
			return fmt.Errorf("tx %d execution failed in block %d: %w", i, header.Number.Uint64(), err)
		}
		s.logger.Debug("tx executed", "block", header.Number, "txIdx", i, "gasUsed", receipt.GasUsed, "status", receipt.Status)
	}

	return doms.Flush(s.ctx, temporalTx)
}
