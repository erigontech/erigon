package aa

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
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

	//baseFee := uint256.MustFromBig(header.BaseFee)

	//effectiveGasPrice := new(uint256.Int).Add(baseFee, tx.GetEffectiveGasTip(baseFee))
	//gasLimit, preCharge, err := BuyGasRip7560Transaction(tx, statedb, effectiveGasPrice, gasPool)

	// TODO: configure tracer

	// TODO: Nonce manager frame
	// applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */)

	// Deployer frame
	msg := tx.DeployerFrame()
	validateDeployer := func(ibs evmtypes.IntraBlockState, epc *core.EntryPointCall) error {
		senderCodeSize, err := ibs.GetCodeSize(*tx.SenderAddress)
		if err != nil {
			return wrapError(fmt.Errorf(
				"error getting code for sender:%s err:%s",
				tx.SenderAddress.String(), err,
			))
		}
		if senderCodeSize == 0 {
			return wrapError(fmt.Errorf(
				"sender not deployed by the deployer, sender:%s deployer:%s",
				tx.SenderAddress.String(), tx.Deployer.String(),
			))
		}
		return nil
	}
	applyRes, err := core.ApplyFrame(evm, msg, gasPool, validateDeployer)
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

	deploymentGasUsed := applyRes.UsedGas

	// Validation frame
	msg, err = tx.ValidationFrame(chainConfig.ChainID, deploymentGasUsed)
	if err != nil {
		return nil, 0, err
	}
	validateValidation := func(ibs evmtypes.IntraBlockState, epc *core.EntryPointCall) error {
		if epc.Error != nil {
			return epc.Error
		}
		if epc.Input == nil {
			return errors.New("account validation did not call the EntryPoint 'acceptAccount' callback")
		}
		if bytes.Compare(epc.From[:], tx.SenderAddress[:]) == 0 {
			return errors.New("invalid call to EntryPoint contract from a wrong account address")
		}

		validityTimeRange, err := DecodeAcceptAccount(epc.Input)
		if err != nil {
			return err
		}
		return validateValidityTimeRange(header.Time, validityTimeRange.ValidAfter, validityTimeRange.ValidUntil)
	}
	applyRes, err = core.ApplyFrame(evm, msg, gasPool, validateValidation)
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
	validationGasUsed += applyRes.UsedGas

	// Paymaster frame
	msg, err = tx.PaymasterFrame(chainConfig.ChainID)
	if err != nil {
		return nil, 0, err
	}

	if msg != nil {
		validatePaymaster := func(ibs evmtypes.IntraBlockState, epc *core.EntryPointCall) error {
			if epc.Error != nil {
				return epc.Error
			}
			if epc.Input == nil {
				return errors.New("paymaster validation did not call the EntryPoint 'acceptPaymaster' callback")
			}

			if bytes.Compare(epc.From[:], tx.Paymaster[:]) != 0 {
				return errors.New("invalid call to EntryPoint contract from a wrong paymaster address")
			}
			paymasterValidity, err := DecodeAcceptPaymaster(epc.Input) // TODO: find better name
			if err != nil {
				return err
			}

			if err = validateValidityTimeRange(header.Time, paymasterValidity.ValidAfter, paymasterValidity.ValidUntil); err != nil {
				return err
			}

			if len(paymasterValidity.Context) > 0 && tx.PostOpGasLimit == 0 {
				return wrapError(
					fmt.Errorf(
						"paymaster returned a context of size %d but the paymasterPostOpGasLimit is 0",
						len(paymasterValidity.Context),
					),
				)
			}

			paymasterContext = paymasterValidity.Context
			return nil
		}
		applyRes, err = core.ApplyFrame(evm, msg, gasPool, validatePaymaster)
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
) (executionStatus uint64, executionReturnData []byte, postOpReturnData []byte, err error) {
	executionStatus = types.ExecutionStatusSuccess

	// Execution frame
	msg := tx.ExecutionFrame()
	applyRes, err := core.ApplyFrame(evm, msg, gasPool, nil)
	if err != nil {
		return 0, nil, nil, err
	}

	if applyRes.Failed() {
		executionStatus = types.ExecutionStatusExecutionFailure
	}
	executionReturnData = applyRes.ReturnData

	executionGasPenalty := (tx.Gas - applyRes.UsedGas) * types.AA_GAS_PENALTY_PCT / 100
	gasUsed := validationGasUsed + applyRes.UsedGas + executionGasPenalty
	gasRefund := capRefund(applyRes.UsedGas+validationGasUsed, gasUsed) // TODO: check correctness, i think this should be moved into statetransition
	finalGasUsed := gasUsed - gasRefund

	// Paymaster post-op frame
	msg, err = tx.PaymasterPostOp(paymasterContext, finalGasUsed, !applyRes.Failed())
	if err != nil {
		return 0, nil, nil, err
	}

	applyRes, err = core.ApplyFrame(evm, msg, gasPool, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if applyRes.Failed() {
		if executionStatus == types.ExecutionStatusExecutionFailure {
			executionStatus = types.ExecutionStatusExecutionFailure
		} else {
			executionStatus = types.ExecutionStatusPostOpFailure
		}

		// TODO: cap refund, unused gas penalty

		return 0, nil, nil, errors.New("paymaster post-op failed")
	}
	postOpReturnData = applyRes.ReturnData

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
	topics, data, err := EncodeRIP7560AccountDeployedEvent(txn.Paymaster, txn.Deployer, txn.SenderAddress)
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
	topics, data, err := EncodeRIP7560TransactionRevertReasonEvent(revertData, txn.Nonce, txn.NonceKey, txn.SenderAddress)
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
	topics, data, err := EncodeRIP7560TransactionPostOpRevertReasonEvent(revertData, txn.Nonce, txn.NonceKey, txn.Paymaster, txn.SenderAddress)
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
	topics, data, err := EncodeRIP7560TransactionEvent(executionStatus, txn.Nonce, txn.NonceKey, txn.Paymaster, txn.Deployer, txn.SenderAddress)
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
