package aa

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
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

	var paymasterCodeSize, deployerCodeSize int

	if tx.Paymaster != nil {
		paymasterCodeSize, err = ibs.GetCodeSize(*tx.Paymaster)
		if err != nil {
			return nil, 0, err
		}
	}

	if tx.Deployer != nil {
		deployerCodeSize, err = ibs.GetCodeSize(*tx.Deployer)
		if err != nil {
			return nil, 0, err
		}
	}

	if err := PerformTxnStaticValidation(tx, senderCodeSize, paymasterCodeSize, deployerCodeSize); err != nil {
		return nil, 0, err
	}

	validationGasUsed = 0

	if err = chargeGas(header, tx, gasPool, ibs); err != nil {
		return nil, 0, err
	}

	originalEvmHooks := evm.Config().Tracer.OnEnter
	entryPointTracer := EntryPointTracer{OnEnterSuper: originalEvmHooks}
	evm.Config().Tracer.OnEnter = entryPointTracer.OnEnter
	defer func() {
		evm.Config().Tracer.OnEnter = originalEvmHooks
	}()

	// TODO: Nonce manager frame
	// applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */)

	// Deployer frame
	msg := tx.DeployerFrame()
	applyRes, err := core.ApplyFrame(evm, msg, gasPool)
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
	if err := deployValidation(tx, ibs); err != nil {
		return nil, 0, err
	}

	deploymentGasUsed := applyRes.UsedGas

	// Validation frame
	msg, err = tx.ValidationFrame(chainConfig.ChainID, deploymentGasUsed)
	if err != nil {
		return nil, 0, err
	}
	applyRes, err = core.ApplyFrame(evm, msg, gasPool)
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
	if err := validationValidation(tx, header, entryPointTracer); err != nil {
		return nil, 0, err
	}
	entryPointTracer.Reset()

	validationGasUsed += applyRes.UsedGas

	// Paymaster frame
	msg, err = tx.PaymasterFrame(chainConfig.ChainID)
	if err != nil {
		return nil, 0, err
	}

	if msg != nil {
		applyRes, err = core.ApplyFrame(evm, msg, gasPool)
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
		paymasterContext, err = paymasterValidation(tx, header, entryPointTracer)
		if err != nil {
			return nil, 0, err
		}
		entryPointTracer.Reset()
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

func deployValidation(tx *types.AccountAbstractionTransaction, ibs *state.IntraBlockState) error {
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

func validationValidation(tx *types.AccountAbstractionTransaction, header *types.Header, ept EntryPointTracer) error {
	if ept.Error != nil {
		return ept.Error
	}
	if ept.Input == nil {
		return errors.New("account validation did not call the EntryPoint 'acceptAccount' callback")
	}
	if bytes.Compare(ept.From[:], tx.SenderAddress[:]) == 0 {
		return errors.New("invalid call to EntryPoint contract from a wrong account address")
	}

	validityTimeRange, err := types.DecodeAcceptAccount(ept.Input)
	if err != nil {
		return err
	}
	return validateValidityTimeRange(header.Time, validityTimeRange.ValidAfter, validityTimeRange.ValidUntil)
}

func paymasterValidation(tx *types.AccountAbstractionTransaction, header *types.Header, epc EntryPointTracer) ([]byte, error) {
	if epc.Error != nil {
		return nil, epc.Error
	}
	if epc.Input == nil {
		return nil, errors.New("paymaster validation did not call the EntryPoint 'acceptPaymaster' callback")
	}

	if bytes.Compare(epc.From[:], tx.Paymaster[:]) != 0 {
		return nil, errors.New("invalid call to EntryPoint contract from a wrong paymaster address")
	}
	paymasterValidity, err := types.DecodeAcceptPaymaster(epc.Input) // TODO: find better name
	if err != nil {
		return nil, err
	}

	if err = validateValidityTimeRange(header.Time, paymasterValidity.ValidAfter, paymasterValidity.ValidUntil); err != nil {
		return nil, err
	}

	if len(paymasterValidity.Context) > 0 && tx.PostOpGasLimit == 0 {
		return nil, wrapError(
			fmt.Errorf(
				"paymaster returned a context of size %d but the paymasterPostOpGasLimit is 0",
				len(paymasterValidity.Context),
			),
		)
	}

	return paymasterValidity.Context, nil
}

func ExecuteAATransaction(
	tx *types.AccountAbstractionTransaction,
	paymasterContext []byte,
	validationGasUsed uint64,
	gasPool *core.GasPool,
	evm *vm.EVM,
	header *types.Header,
	ibs *state.IntraBlockState,
) (executionStatus uint64, gasUsed uint64, err error) {
	executionStatus = types.ExecutionStatusSuccess

	// Execution frame
	msg := tx.ExecutionFrame()
	applyRes, err := core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return 0, 0, err
	}

	if applyRes.Failed() {
		executionStatus = types.ExecutionStatusExecutionFailure
	}

	execRefund := capRefund(tx.GasLimit-applyRes.UsedGas, applyRes.UsedGas) // TODO: can be moved into statetransition
	validationRefund := capRefund(tx.ValidationGasLimit-validationGasUsed, validationGasUsed)

	executionGasPenalty := (tx.GasLimit - applyRes.UsedGas) * types.AA_GAS_PENALTY_PCT / 100
	gasUsed = validationGasUsed + applyRes.UsedGas + executionGasPenalty
	gasRefund := capRefund(execRefund+validationRefund, gasUsed)

	// Paymaster post-op frame
	msg, err = tx.PaymasterPostOp(paymasterContext, gasUsed, !applyRes.Failed())
	if err != nil {
		return 0, 0, err
	}

	applyRes, err = core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return 0, 0, err
	}
	if applyRes.Failed() {
		if executionStatus == types.ExecutionStatusExecutionFailure {
			executionStatus = types.ExecutionStatusExecutionAndPostOpFailure
		} else {
			executionStatus = types.ExecutionStatusPostOpFailure
		}
	}

	validationGasPenalty := (tx.PostOpGasLimit - applyRes.UsedGas) * types.AA_GAS_PENALTY_PCT / 100
	gasRefund += capRefund(tx.PostOpGasLimit-applyRes.UsedGas, applyRes.UsedGas)
	gasUsed += applyRes.UsedGas + validationGasPenalty

	if err = refundGas(header, tx, ibs, gasUsed-gasRefund); err != nil {
		return 0, 0, err
	}

	if err = payCoinbase(header, tx, ibs, gasUsed-gasRefund, evm.Context.Coinbase); err != nil {
		return 0, 0, err
	}

	gasPool.AddGas(fixedgas.TxAAGas + tx.ValidationGasLimit + tx.PaymasterValidationGasLimit + tx.GasLimit + tx.PostOpGasLimit - gasUsed)

	return executionStatus, gasUsed, nil
}

// TODO: get rid of?
func capRefund(getRefund uint64, gasUsed uint64) uint64 {
	refund := gasUsed / params.RefundQuotientEIP3529
	if refund > getRefund {
		return getRefund
	}
	return refund
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
		contractSubst = " in contract " + revertEntityName
	}
	if innerErr != nil {
		errorMessage = fmt.Sprintf(
			"validation phase failed%s with exception: %s",
			contractSubst,
			innerErr.Error(),
		)
	} else {
		errorMessage = "validation phase failed " + contractSubst
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
