package aa

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

func ValidateAATransaction(
	tx *types.AccountAbstractionTransaction,
	ibs *state.IntraBlockState,
	gasPool *core.GasPool,
	header *types.Header,
	evm *vm.EVM,
	chainConfig *chain.Config,
) (paymasterContext []byte, validationGasUsed uint64, err error) {
	senderCodeSize, err := ibs.GetCodeSize(*tx.SenderAddress)
	if err != nil {
		return nil, 0, err
	}

	paymasterCodeSize, err := ibs.GetCodeSize(*tx.Paymaster)
	if err != nil {
		return nil, 0, err
	}

	deployerCodeSize, err := ibs.GetCodeSize(*tx.Deployer)
	if err != nil {
		return nil, 0, err
	}

	if err := PerformTxnStaticValidation(tx, senderCodeSize, paymasterCodeSize, deployerCodeSize); err != nil {
		return nil, 0, err
	}

	validationGasUsed = 0

	if err = chargeGas(header, tx, gasPool, ibs); err != nil {
		return nil, 0, err
	}

	// TODO: configure tracer

	// TODO: Nonce manager frame
	// applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */)

	// Deployer frame
	msg := tx.DeployerFrame()
	applyRes, _, err := core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return nil, 0, err
	}
	if applyRes.Failed() {
		return nil, 0, newValidationPhaseError(
			applyRes.Err,
			applyRes.ReturnData,
			"deployer",
			true,
		)
	}
	senderCodeSize, err = ibs.GetCodeSize(*tx.SenderAddress)
	if err != nil {
		return nil, 0, wrapError(fmt.Errorf(
			"error getting code for sender:%s err:%s",
			tx.SenderAddress.String(), err,
		))
	}
	if senderCodeSize == 0 {
		return nil, 0, wrapError(fmt.Errorf(
			"sender not deployed by the deployer, sender:%s deployer:%s",
			tx.SenderAddress.String(), tx.Deployer.String(),
		))
	}

	deploymentGasUsed := applyRes.UsedGas

	// Validation frame
	msg, err = tx.ValidationFrame(chainConfig.ChainID, deploymentGasUsed)
	if err != nil {
		return nil, 0, err
	}

	applyRes, aaReturn, err := core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return nil, 0, err
	}
	if applyRes.Failed() {
		return nil, 0, newValidationPhaseError(
			applyRes.Err,
			applyRes.ReturnData,
			"account",
			true,
		)
	}

	// NOTE: seems like anyone can call this and not just the account with this approach.
	// Is this an important security feature? Why was that needed in the first place?
	err = validateValidityTimeRange(header.Time, aaReturn.ValidAfter, aaReturn.ValidUntil)
	if err != nil {
		return nil, 0, err
	}
	validationGasUsed += applyRes.UsedGas

	// Paymaster frame
	msg, err = tx.PaymasterFrame(chainConfig.ChainID)
	if err != nil {
		return nil, 0, err
	}

	if msg != nil {
		applyRes, aaReturn, err = core.ApplyFrame(evm, msg, gasPool)
		if err != nil {
			return nil, 0, err
		}
		if applyRes.Failed() {
			return nil, 0, newValidationPhaseError(
				applyRes.Err,
				applyRes.ReturnData,
				"paymaster",
				true,
			)
		}
		validationGasUsed += applyRes.UsedGas

		if err = validateValidityTimeRange(header.Time, aaReturn.ValidAfter, aaReturn.ValidUntil); err != nil {
			return nil, 0, err
		}

		if len(aaReturn.PaymasterContext) > 0 && tx.PostOpGasLimit == 0 {
			return nil, 0, wrapError(
				fmt.Errorf(
					"paymaster returned a context of size %d but the paymasterPostOpGasLimit is 0",
					len(aaReturn.PaymasterContext),
				),
			)
		}

		paymasterContext = aaReturn.PaymasterContext
	}

	return paymasterContext, validationGasUsed, nil
}

func validateValidityTimeRange(time uint64, validAfter uint64, validUntil uint64) error {
	if validUntil == 0 && validAfter == 0 {
		return nil
	}
	if validUntil < validAfter {
		return errors.New("RIP-7560 transaction validity range invalid")
	}
	if time > validUntil {
		return errors.New("RIP-7560 transaction validity expired")
	}
	if time < validAfter {
		return errors.New("RIP-7560 transaction validity not reached yet")
	}
	return nil
}

func ExecuteAATransaction(
	tx *types.AccountAbstractionTransaction,
	paymasterContext []byte,
	validationGasUsed uint64,
	gasPool *core.GasPool,
	evm *vm.EVM,
	header *types.Header,
	ibs *state.IntraBlockState,
) (executionStatus uint64, executionReturnData []byte, postOpReturnData []byte, err error) {
	executionStatus = types.ExecutionStatusSuccess

	// Execution frame
	msg := tx.ExecutionFrame()
	applyRes, _, err := core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return 0, nil, nil, err
	}

	if applyRes.Failed() {
		executionStatus = types.ExecutionStatusExecutionFailure
	}
	executionReturnData = applyRes.ReturnData

	execRefund := capRefund(tx.Gas-applyRes.UsedGas, applyRes.UsedGas) // TODO: can be moved into statetransition
	validationRefund := capRefund(tx.ValidationGasLimit-validationGasUsed, validationGasUsed)

	executionGasPenalty := (tx.Gas - applyRes.UsedGas) * types.AA_GAS_PENALTY_PCT / 100
	gasUsed := validationGasUsed + applyRes.UsedGas + executionGasPenalty
	gasRefund := capRefund(execRefund+validationRefund, gasUsed)

	// Paymaster post-op frame
	msg, err = tx.PaymasterPostOp(paymasterContext, gasUsed, !applyRes.Failed())
	if err != nil {
		return 0, nil, nil, err
	}

	applyRes, _, err = core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return 0, nil, nil, err
	}
	if applyRes.Failed() {
		if executionStatus == types.ExecutionStatusExecutionFailure {
			executionStatus = types.ExecutionStatusExecutionFailure
		} else {
			executionStatus = types.ExecutionStatusPostOpFailure
		}

		validationGasPenalty := (tx.PostOpGasLimit - applyRes.UsedGas) * types.AA_GAS_PENALTY_PCT / 100
		gasRefund += capRefund(tx.PostOpGasLimit-applyRes.UsedGas, applyRes.UsedGas)
		gasUsed += applyRes.UsedGas + validationGasPenalty

		return 0, nil, nil, errors.New("paymaster post-op failed")
	}
	postOpReturnData = applyRes.ReturnData

	if err = refundGas(header, tx, ibs, gasUsed-gasRefund); err != nil {
		return 0, nil, nil, err
	}

	if err = payCoinbase(header, tx, ibs, gasUsed-gasRefund, evm.Context.Coinbase); err != nil {
		return 0, nil, nil, err
	}

	gasPool.AddGas(fixedgas.TxAAGas + tx.ValidationGasLimit + tx.PaymasterValidationGasLimit + tx.Gas + tx.PostOpGasLimit - gasUsed)

	return executionStatus, executionReturnData, postOpReturnData, nil
}

// TODO: get rid of?
func capRefund(getRefund uint64, gasUsed uint64) uint64 {
	refund := gasUsed / params.RefundQuotientEIP3529
	if refund > getRefund {
		return getRefund
	}
	return refund
}

func InjectAALogs(tx *types.AccountAbstractionTransaction, executionStatus, blockNum uint64, execReturnData, postOpReturnData []byte, ibs *state.IntraBlockState) error {
	err := injectRIP7560TransactionEvent(tx, executionStatus, blockNum, ibs)
	if err != nil {
		return err
	}
	if tx.Deployer != nil {
		err = injectRIP7560AccountDeployedEvent(tx, blockNum, ibs)
		if err != nil {
			return err
		}
	}
	if executionStatus == types.ExecutionStatusExecutionFailure || executionStatus == types.ExecutionStatusExecutionAndPostOpFailure {
		err = injectRIP7560TransactionRevertReasonEvent(tx, execReturnData, blockNum, ibs)
		if err != nil {
			return err
		}
	}
	if executionStatus == types.ExecutionStatusPostOpFailure || executionStatus == types.ExecutionStatusExecutionAndPostOpFailure {
		err = injectRIP7560TransactionPostOpRevertReasonEvent(tx, postOpReturnData, blockNum, ibs)
		if err != nil {
			return err
		}
	}
	return nil
}

func injectRIP7560AccountDeployedEvent(
	txn *types.AccountAbstractionTransaction,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := types.EncodeRIP7560AccountDeployedEvent(txn.Paymaster, txn.Deployer, txn.SenderAddress)
	if err != nil {
		return err
	}
	err = injectEvent(topics, data, blockNum, ibs)
	if err != nil {
		return err
	}
	return nil
}

func injectRIP7560TransactionRevertReasonEvent(
	txn *types.AccountAbstractionTransaction,
	revertData []byte,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := types.EncodeRIP7560TransactionRevertReasonEvent(revertData, txn.Nonce, txn.NonceKey, txn.SenderAddress)
	if err != nil {
		return err
	}
	err = injectEvent(topics, data, blockNum, ibs)
	if err != nil {
		return err
	}
	return nil
}

func injectRIP7560TransactionPostOpRevertReasonEvent(
	txn *types.AccountAbstractionTransaction,
	revertData []byte,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := types.EncodeRIP7560TransactionPostOpRevertReasonEvent(revertData, txn.Nonce, txn.NonceKey, txn.Paymaster, txn.SenderAddress)
	if err != nil {
		return err
	}
	err = injectEvent(topics, data, blockNum, ibs)
	if err != nil {
		return err
	}
	return nil
}

func injectRIP7560TransactionEvent(
	txn *types.AccountAbstractionTransaction,
	executionStatus uint64,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := types.EncodeRIP7560TransactionEvent(executionStatus, txn.Nonce, txn.NonceKey, txn.Paymaster, txn.Deployer, txn.SenderAddress)
	if err != nil {
		return err
	}
	err = injectEvent(topics, data, blockNum, ibs)
	if err != nil {
		return err
	}
	return nil
}

func injectEvent(topics []common.Hash, data []byte, blockNumber uint64, ibs *state.IntraBlockState) error {
	transactionLog := &types.Log{
		Address:     types.AA_ENTRY_POINT,
		Topics:      topics,
		Data:        data,
		BlockNumber: blockNumber,
	}
	ibs.AddLog(transactionLog)
	return nil
}

func PerformTxnStaticValidation(
	txn *types.AccountAbstractionTransaction,
	senderCodeSize, paymasterCodeSize, deployerCodeSize int,
) error {
	paymasterAddress, deployerAddress, senderAddress := txn.Paymaster, txn.Deployer, txn.SenderAddress
	paymasterData, deployerData, paymasterValidationGasLimit := txn.PaymasterData, txn.DeployerData, txn.PaymasterValidationGasLimit

	return txpool.AAStaticValidation(
		paymasterAddress, deployerAddress, senderAddress,
		paymasterData, deployerData,
		paymasterValidationGasLimit,
		senderCodeSize, paymasterCodeSize, deployerCodeSize,
	)
}

// ValidationPhaseError is an API error that encompasses an EVM revert with JSON error
// code and a binary data blob. Only used for debug, can remove.
type ValidationPhaseError struct {
	error
	reason string // revert reason hex encoded

	revertEntityName string
	frameReverted    bool
}

func (v *ValidationPhaseError) ErrorData() interface{} {
	return v.reason
}

// wrapError creates a revertError instance for validation errors not caused by an on-chain revert
func wrapError(
	innerErr error,
) *ValidationPhaseError {
	return newValidationPhaseError(innerErr, nil, "", false)
}

// newValidationPhaseError creates a revertError instance with the provided revert data.
func newValidationPhaseError(
	innerErr error,
	revertReason []byte,
	revertEntityName string,
	frameReverted bool,
) *ValidationPhaseError {
	var vpeCast *ValidationPhaseError
	if errors.As(innerErr, &vpeCast) {
		return vpeCast
	}
	var errorMessage string
	contractSubst := ""
	if revertEntityName != "" {
		contractSubst = fmt.Sprintf(" in contract %s", revertEntityName)
	}
	if innerErr != nil {
		errorMessage = fmt.Sprintf(
			"validation phase failed%s with exception: %s",
			contractSubst,
			innerErr.Error(),
		)
	} else {
		errorMessage = fmt.Sprintf("validation phase failed%s", contractSubst)
	}
	// TODO: use "vm.ErrorX" for RIP-7560 specific errors as well!
	err := errors.New(errorMessage)

	reason, errUnpack := abi.UnpackRevert(revertReason)
	if errUnpack == nil {
		err = fmt.Errorf("%w: %v", err, reason)
	}
	return &ValidationPhaseError{
		error:  err,
		reason: hex.EncodeToString(revertReason),

		frameReverted:    frameReverted,
		revertEntityName: revertEntityName,
	}
}
