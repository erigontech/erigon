package aa

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/types"
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

	vmConfig := evm.Config()
	rules := evm.ChainRules()
	hasEIP3860 := vmConfig.HasEip3860(rules)

	preTxCost, err := tx.PreTransactionGasCost(rules, hasEIP3860)
	if err != nil {
		return nil, 0, err
	}
	validationGasUsed = preTxCost

	if err = chargeGas(header, tx, gasPool, ibs, preTxCost); err != nil {
		return nil, 0, err
	}

	var originalEvmHook tracing.EnterHook
	entryPointTracer := EntryPointTracer{}
	if vmConfig.Tracer != nil && vmConfig.Tracer.OnEnter != nil {
		entryPointTracer = EntryPointTracer{OnEnterSuper: originalEvmHook}
	}
	vmConfig.Tracer = entryPointTracer.Hooks()
	innerEvm := vm.NewEVM(evm.Context, evm.TxContext, ibs, evm.ChainConfig(), vmConfig)

	// TODO: Nonce manager frame
	// applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */)

	senderNonce, _ := ibs.GetNonce(*tx.SenderAddress)
	if tx.Nonce > senderNonce+1 { // ibs returns last used nonce
		return nil, 0, errors.New("nonce too low")
	}

	// Deployer frame
	msg := tx.DeployerFrame(rules, hasEIP3860)
	applyRes, err := core.ApplyFrame(innerEvm, msg, gasPool)
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
	entryPointTracer.Reset()

	deploymentGasUsed := applyRes.GasUsed

	// Validation frame
	msg, err = tx.ValidationFrame(chainConfig.ChainID, deploymentGasUsed, rules, hasEIP3860)
	if err != nil {
		return nil, 0, err
	}
	applyRes, err = core.ApplyFrame(innerEvm, msg, gasPool)
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

	validationGasUsed += applyRes.GasUsed

	// Paymaster frame
	msg, err = tx.PaymasterFrame(chainConfig.ChainID)
	if err != nil {
		return nil, 0, err
	}

	if msg != nil {
		applyRes, err = core.ApplyFrame(innerEvm, msg, gasPool)
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
		validationGasUsed += applyRes.GasUsed
	}

	log.Info("validation gas report", "gasUsed", validationGasUsed, "nonceManager", 0, "refund", 0, "pretransactioncost", preTxCost)

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
	if !bytes.Equal(ept.From[:], tx.SenderAddress[:]) {
		return fmt.Errorf("invalid call to EntryPoint contract from a wrong account address, wanted %s got %s", tx.SenderAddress.String(), ept.From)
	}

	validityTimeRange, err := types.DecodeAcceptAccount(ept.Input)
	if err != nil {
		return err
	}
	return validateValidityTimeRange(header.Time, validityTimeRange.ValidAfter.Uint64(), validityTimeRange.ValidUntil.Uint64())
}

func paymasterValidation(tx *types.AccountAbstractionTransaction, header *types.Header, ept EntryPointTracer) ([]byte, error) {
	if ept.Error != nil {
		return nil, ept.Error
	}
	if ept.Input == nil {
		return nil, errors.New("paymaster validation did not call the EntryPoint 'acceptPaymaster' callback")
	}

	if !bytes.Equal(ept.From[:], tx.Paymaster[:]) {
		return nil, errors.New("invalid call to EntryPoint contract from a wrong paymaster address")
	}
	paymasterValidity, err := types.DecodeAcceptPaymaster(ept.Input) // TODO: find better name
	if err != nil {
		return nil, err
	}

	if err = validateValidityTimeRange(header.Time, paymasterValidity.ValidAfter.Uint64(), paymasterValidity.ValidUntil.Uint64()); err != nil {
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

	nonce, err := ibs.GetNonce(*tx.SenderAddress)
	if err != nil {
		return 0, 0, err
	}
	if err = ibs.SetNonce(*tx.SenderAddress, nonce+1); err != nil {
		return 0, 0, err
	}

	// Execution frame
	msg := tx.ExecutionFrame()
	applyRes, err := core.ApplyFrame(evm, msg, gasPool)
	if err != nil {
		return 0, 0, err
	}

	if applyRes.Failed() {
		executionStatus = types.ExecutionStatusExecutionFailure
	}

	execRefund := capRefund(tx.GasLimit-applyRes.GasUsed, applyRes.GasUsed) // TODO: can be moved into statetransition
	validationRefund := capRefund(tx.ValidationGasLimit-validationGasUsed, validationGasUsed)

	executionGasPenalty := (tx.GasLimit - applyRes.GasUsed) * types.AA_GAS_PENALTY_PCT / 100
	gasUsed = validationGasUsed + applyRes.GasUsed + executionGasPenalty
	gasRefund := capRefund(execRefund+validationRefund, gasUsed)
	log.Info("execution gas used", "gasUsed", applyRes.GasUsed, "penalty", executionGasPenalty)

	// Paymaster post-op frame
	if len(paymasterContext) != 0 {
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

		validationGasPenalty := (tx.PostOpGasLimit - applyRes.GasUsed) * types.AA_GAS_PENALTY_PCT / 100
		gasRefund += capRefund(tx.PostOpGasLimit-applyRes.GasUsed, applyRes.GasUsed)
		gasUsed += applyRes.GasUsed + validationGasPenalty
		log.Info("post op gas used", "gasUsed", applyRes.GasUsed, "penalty", validationGasPenalty)
	}

	if err = refundGas(header, tx, ibs, gasUsed-gasRefund); err != nil {
		return 0, 0, err
	}

	if err = payCoinbase(header, tx, ibs, gasUsed-gasRefund, evm.Context.Coinbase); err != nil {
		return 0, 0, err
	}

	gasPool.AddGas(params.TxAAGas + tx.ValidationGasLimit + tx.PaymasterValidationGasLimit + tx.GasLimit + tx.PostOpGasLimit - gasUsed)

	return executionStatus, gasUsed, nil
}

func CreateAAReceipt(txnHash common.Hash, status, gasUsed, cumGasUsed, blockNum, txnIndex uint64, logs types.Logs) *types.Receipt {
	receipt := &types.Receipt{Type: types.AccountAbstractionTxType, CumulativeGasUsed: cumGasUsed}
	receipt.Status = status
	receipt.TxHash = txnHash
	receipt.GasUsed = gasUsed
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = logs
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockNumber = big.NewInt(int64(blockNum))
	receipt.TransactionIndex = uint(txnIndex)

	return receipt
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
	hasPaymaster := txn.Paymaster != nil
	hasPaymasterData := len(txn.PaymasterData) != 0
	hasPaymasterGasLimit := txn.PaymasterValidationGasLimit != 0
	hasDeployer := txn.Deployer != nil
	hasDeployerData := len(txn.DeployerData) != 0
	hasCodeSender := senderCodeSize != 0
	hasCodeDeployer := deployerCodeSize != 0

	if !hasDeployer && hasDeployerData {
		return fmt.Errorf(
			"deployer data of size %d is provided but deployer address is not set",
			len(txn.DeployerData),
		)

	}
	if !hasPaymaster && (hasPaymasterData || hasPaymasterGasLimit) {
		return fmt.Errorf(
			"paymaster data of size %d (or a gas limit: %d) is provided but paymaster address is not set",
			len(txn.DeployerData), txn.PaymasterValidationGasLimit,
		)

	}

	if hasPaymaster {
		if !hasPaymasterGasLimit {
			return fmt.Errorf(
				"paymaster address  %s is provided but 'paymasterVerificationGasLimit' is zero",
				txn.Paymaster.String(),
			)

		}
		hasCodePaymaster := paymasterCodeSize != 0
		if !hasCodePaymaster {
			return fmt.Errorf(
				"paymaster address %s is provided but contract has no code deployed",
				txn.Paymaster.String(),
			)

		}
	}

	if hasDeployer {
		if !hasCodeDeployer {
			return fmt.Errorf(
				"deployer address %s is provided but contract has no code deployed",
				txn.Deployer.String(),
			)

		}
		if hasCodeSender {
			return fmt.Errorf(
				"sender address %s and deployer address %s are provided but sender is already deployed",
				txn.SenderAddress.String(), txn.Deployer.String(),
			)
		}
	}

	if !hasDeployer && !hasCodeSender {
		return errors.New("account is not deployed and no deployer is specified")
	}

	return nil
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
