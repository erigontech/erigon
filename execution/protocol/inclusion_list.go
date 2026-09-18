package protocol

import (
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

func CheckInclusionListTransactions(
	evm *vm.EVM,
	gp *GasPool,
	signer types.Signer,
	blockTxns types.Transactions,
	inclusionListTxns types.Transactions,
) bool {

	inBlock := make(map[common.Hash]struct{}, len(blockTxns))
	for _, tx := range blockTxns {
		inBlock[tx.Hash()] = struct{}{}
	}

	rules := evm.ChainRules()
	baseFee := evm.Context.BaseFee

	for _, tx := range inclusionListTxns {
		if _, ok := inBlock[tx.Hash()]; ok {
			continue
		}

		msg, err := tx.AsMessage(signer, &baseFee, rules)
		if err != nil {
			continue
		}

		executor := NewTxnExecutor(evm, msg, gp)

		intrinsicGasResult, overflow := executor.calcIntrinsicGas(
			msg.To().IsNil(),
			msg.Authorizations(),
			slices.Clone[types.AccessList](msg.AccessList()),
		)
		if overflow {
			continue
		}

		if _, err := executor.preCheck(false, intrinsicGasResult); err != nil {
			continue
		}

		log.Warn("[EIP-7805]: Inclusion List transaction should have been included in the block")
		log.Warn("txn:", tx.Hash(), "sender:", msg.From(), "nonce:", msg.Nonce(), "block:", evm.Context.BlockNumber)

		return false
	}

	return true
}
