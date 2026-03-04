package protocol

import (
	"fmt"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// RevertedTxGasUsed maps known Arbitrum transaction hashes that reverted
// to their actual gas used. These are historical mainnet transactions
// where gas accounting differs from standard EVM execution.
var RevertedTxGasUsed = map[common.Hash]uint64{
	common.HexToHash("0x58df300a7f04fe31d41d24672786cbe1c58b4f3d8329d0d74392d814dd9f7e40"): 45174,
}

// handleRevertedTx attempts to process a reverted transaction. It returns
// ErrExecutionReverted with the updated multiGas if a matching reverted
// tx is found; otherwise, it returns nil error with unchanged multiGas.
func (st *StateTransition) handleRevertedTx(msg *types.Message, usedMultiGas multigas.MultiGas) (multigas.MultiGas, error) {
	if msg.Tx == nil {
		return usedMultiGas, nil
	}

	txHash := msg.Tx.Hash()
	if l2GasUsed, ok := RevertedTxGasUsed[txHash]; ok {
		pn, err := st.state.GetNonce(msg.From())
		if err != nil {
			return usedMultiGas, fmt.Errorf("handle revert: %w", err)
		}
		err = st.state.SetNonce(msg.From(), pn+1)
		if err != nil {
			return usedMultiGas, fmt.Errorf("handle revert: %w", err)
		}

		if l2GasUsed < params.TxGas {
			return usedMultiGas, fmt.Errorf("adjustedGas underflow in handleRevertedTx: l2GasUsed=%d, params.TxGas=%d", l2GasUsed, params.TxGas)
		}
		adjustedGas := l2GasUsed - params.TxGas
		if st.gasRemaining < adjustedGas {
			return usedMultiGas, fmt.Errorf("gasRemaining underflow in handleRevertedTx: gasRemaining=%d, adjustedGas=%d", st.gasRemaining, adjustedGas)
		}
		st.gasRemaining -= adjustedGas

		usedMultiGas = usedMultiGas.SaturatingAdd(multigas.ComputationGas(adjustedGas))
		return usedMultiGas, vm.ErrExecutionReverted
	}

	return usedMultiGas, nil
}
