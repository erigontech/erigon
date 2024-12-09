package types

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	rlp2 "github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/accounts/abi"
	libcommon "github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	types "github.com/erigontech/erigon/core/types/aa"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/params"

	"github.com/erigontech/erigon/rlp"
)

const (
	ExecutionStatusSuccess                   = uint64(0)
	ExecutionStatusExecutionFailure          = uint64(1)
	ExecutionStatusPostOpFailure             = uint64(2)
	ExecutionStatusExecutionAndPostOpFailure = uint64(3)
)

var AA_ENTRY_POINT = common.HexToAddress("0x0000000000000000000000000000000000007560")
var AA_SENDER_CREATOR = common.HexToAddress("0x00000000000000000000000000000000ffff7560")

const AA_GAS_PENALTY_PCT = 10

type AccountAbstractionTransaction struct {
	DynamicFeeTransaction
	SenderAddress               *common.Address
	AuthorizationData           []Authorization
	ExecutionData               []byte
	Paymaster                   *common.Address `rlp:"nil"`
	PaymasterData               []byte
	Deployer                    *common.Address `rlp:"nil"`
	DeployerData                []byte
	BuilderFee                  *uint256.Int // NOTE: this is *big.Int in geth impl
	ValidationGasLimit          uint64
	PaymasterValidationGasLimit uint64
	PostOpGasLimit              uint64

	// RIP-7712 two-dimensional nonce (optional), 192 bits
	NonceKey *uint256.Int // NOTE: this is *big.Int in geth impl
}

func (tx *AccountAbstractionTransaction) copy() *AccountAbstractionTransaction {
	cpy := &AccountAbstractionTransaction{
		SenderAddress:               &*tx.SenderAddress,
		ExecutionData:               common.CopyBytes(tx.ExecutionData),
		Paymaster:                   &*tx.Paymaster,
		PaymasterData:               common.CopyBytes(tx.PaymasterData),
		Deployer:                    &*tx.Deployer,
		DeployerData:                common.CopyBytes(tx.DeployerData),
		BuilderFee:                  new(uint256.Int),
		ValidationGasLimit:          tx.ValidationGasLimit,
		PaymasterValidationGasLimit: tx.PaymasterValidationGasLimit,
		PostOpGasLimit:              tx.PostOpGasLimit,
		NonceKey:                    new(uint256.Int),
	}
	cpy.DynamicFeeTransaction = *tx.DynamicFeeTransaction.copy()

	cpy.AuthorizationData = make([]Authorization, len(tx.AuthorizationData))
	for i, ath := range tx.AuthorizationData {
		cpy.AuthorizationData[i] = *ath.copy()
	}

	if tx.BuilderFee != nil {
		cpy.BuilderFee.Set(tx.BuilderFee)
	}
	if tx.NonceKey != nil {
		cpy.NonceKey.Set(tx.NonceKey)
	}
	return cpy
}

func (tx *AccountAbstractionTransaction) Type() byte {
	return AccountAbstractionTxType
}

// NOTE: DO NOT USE
func (tx *AccountAbstractionTransaction) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg := Message{
		nonce:      tx.Nonce, // may need 7712 handling
		gasLimit:   tx.Gas,
		gasPrice:   *tx.FeeCap,
		tip:        *tx.Tip,
		feeCap:     *tx.FeeCap,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: tx.AccessList,
		checkNonce: true,
	}
	if !rules.IsPolygonAA {
		return msg, errors.New("AccountAbstractionTransaction transactions require AA to be enabled")
	}
	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return msg, errors.New("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, tx.Tip)
	if msg.gasPrice.Gt(tx.FeeCap) {
		msg.gasPrice.Set(tx.FeeCap)
	}

	var err error
	msg.from, err = tx.Sender(s)
	return msg, err
}

func (tx *AccountAbstractionTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	return tx, nil
}

func (tx *AccountAbstractionTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(AccountAbstractionTxType, []interface{}{
		tx.ChainID,
		tx.NonceKey, tx.Nonce,
		tx.SenderAddress,
		tx.Deployer, tx.DeployerData,
		tx.Paymaster, tx.PaymasterData,
		tx.ExecutionData,
		tx.BuilderFee,
		tx.Tip, tx.FeeCap,
		tx.ValidationGasLimit, tx.PaymasterValidationGasLimit, tx.PostOpGasLimit,
		tx.Gas,
		tx.AccessList,
		tx.AuthorizationData,
	})

	tx.hash.Store(&hash)
	return hash
}

func (tx *AccountAbstractionTransaction) SigningHash(chainID *big.Int) common.Hash {
	hash := prefixedRlpHash(AccountAbstractionTxType, []interface{}{
		chainID,
		tx.NonceKey, tx.Nonce,
		tx.SenderAddress,
		tx.Deployer, tx.DeployerData,
		tx.Paymaster, tx.PaymasterData,
		tx.ExecutionData,
		tx.BuilderFee,
		tx.Tip, tx.FeeCap,
		tx.ValidationGasLimit, tx.PaymasterValidationGasLimit, tx.PostOpGasLimit,
		tx.Gas,
		tx.AccessList,
	})

	tx.hash.Store(&hash)
	return hash
}

func (tx *AccountAbstractionTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return new(uint256.Int), new(uint256.Int), new(uint256.Int)
}

func (tx *AccountAbstractionTransaction) payloadSize() (payloadSize, nonceLen, gasLen, accessListLen int) {
	payloadSize, nonceLen, gasLen, accessListLen = tx.DynamicFeeTransaction.payloadSize()

	payloadSize++
	if tx.SenderAddress != nil {
		payloadSize += 20
	}

	authorizationsLen := authorizationsSize(tx.AuthorizationData)
	payloadSize += rlp2.ListPrefixLen(authorizationsLen) + authorizationsLen

	payloadSize++
	payloadSize += rlp2.StringLen(tx.ExecutionData)

	payloadSize++
	if tx.Paymaster != nil {
		payloadSize += 20
	}

	payloadSize++
	payloadSize += rlp2.StringLen(tx.PaymasterData)

	payloadSize++
	if tx.Deployer != nil {
		payloadSize += 20
	}

	payloadSize++
	payloadSize += rlp2.StringLen(tx.DeployerData)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.BuilderFee)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.ValidationGasLimit)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.PaymasterValidationGasLimit)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.PostOpGasLimit)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.NonceKey)

	return
}

func (tx *AccountAbstractionTransaction) EncodingSize() int {
	payloadSize, _, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp2.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *AccountAbstractionTransaction) EncodeRLP(w io.Writer) error {
	zeroAddress := common.Address{}
	txCopy := tx.copy()
	if txCopy.Paymaster != nil && bytes.Compare(zeroAddress[:], txCopy.Paymaster[:]) == 0 {
		txCopy.Paymaster = nil
	}
	if txCopy.Deployer != nil && bytes.Compare(zeroAddress[:], txCopy.Deployer[:]) == 0 {
		txCopy.Deployer = nil
	}
	return rlp.Encode(w, txCopy)
}

func (tx *AccountAbstractionTransaction) DecodeRLP(s *rlp.Stream) error {
	b, err := s.Bytes()
	if err != nil {
		return err
	}
	return rlp.DecodeBytes(b, tx)
}

func (tx *AccountAbstractionTransaction) MarshalBinary(w io.Writer) error {
	return tx.EncodeRLP(w)
}

func (tx *AccountAbstractionTransaction) ValidateAATransaction(
	ibs *state.IntraBlockState,
	gasPool *core.GasPool,
	header *Header,
	evm *vm.EVM,
	chainConfig *chain.Config,
) (paymasterContext []byte, validationGasUsed uint64, err error) {
	senderCodeSize := ibs.GetCodeSize(*tx.SenderAddress)
	paymasterCodeSize := ibs.GetCodeSize(*tx.Paymaster)
	deployerCodeSize := ibs.GetCodeSize(*tx.Deployer)
	if err := performStaticValidation(tx, senderCodeSize, paymasterCodeSize, deployerCodeSize); err != nil {
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
	msg := tx.deployerFrame()
	validateDeployer := func(ibs evmtypes.IntraBlockState, epc *core.EntryPointCall) error {
		if ibs.GetCodeSize(*tx.SenderAddress) == 0 {
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
	msg, err = tx.validationFrame(chainConfig.ChainID, deploymentGasUsed)
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

		validityTimeRange, err := types.AbiDecodeAcceptAccount(epc.Input, false)
		if err != nil {
			return err
		}
		return validateValidityTimeRange(header.Time, validityTimeRange.ValidAfter.Uint64(), validityTimeRange.ValidUntil.Uint64())
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
	msg, err = tx.paymasterFrame(chainConfig.ChainID)
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
			paymasterValidity, err := types.AbiDecodeAcceptPaymaster(epc.Input, false) // TODO: find better name
			if err != nil {
				return err
			}

			if err = validateValidityTimeRange(header.Time, paymasterValidity.ValidAfter.Uint64(), paymasterValidity.ValidUntil.Uint64()); err != nil {
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

func (tx *AccountAbstractionTransaction) deployerFrame() *Message {
	intrinsicGas, _ := tx.PreTransactionGasCost()
	deployerGasLimit := tx.ValidationGasLimit - intrinsicGas
	return &Message{
		to:       tx.Deployer,
		from:     AA_SENDER_CREATOR,
		gasLimit: deployerGasLimit,
		data:     tx.DeployerData,
	}
}

func (tx *AccountAbstractionTransaction) validationFrame(chainID *big.Int, deploymentUsedGas uint64) (*Message, error) {
	signingHash := tx.SigningHash(chainID)
	txAbiEncoding, err := tx.AbiEncode()
	if err != nil {
		return nil, err
	}

	validateTransactionData, err := types.AccountAbstractionABI.Pack("validateTransaction", big.NewInt(types.AccountAbstractionABIVersion), signingHash, txAbiEncoding)
	if err != nil {
		return nil, err
	}

	intrinsicGas, _ := tx.PreTransactionGasCost()
	accountGasLimit := tx.ValidationGasLimit - intrinsicGas - deploymentUsedGas

	return &Message{
		to:       tx.SenderAddress,
		from:     AA_ENTRY_POINT,
		gasLimit: accountGasLimit,
		data:     validateTransactionData,
	}, nil
}

func (tx *AccountAbstractionTransaction) AbiEncode() ([]byte, error) {
	structThing, _ := abi.NewType("tuple", "struct thing", []abi.ArgumentMarshaling{
		{Name: "sender", Type: "address"},
		{Name: "nonceKey", Type: "uint256"},
		{Name: "nonce", Type: "uint256"},
		{Name: "validationGasLimit", Type: "uint256"},
		{Name: "paymasterValidationGasLimit", Type: "uint256"},
		{Name: "postOpGasLimit", Type: "uint256"},
		{Name: "callGasLimit", Type: "uint256"},
		{Name: "maxFeePerGas", Type: "uint256"},
		{Name: "maxPriorityFeePerGas", Type: "uint256"},
		{Name: "builderFee", Type: "uint256"},
		{Name: "paymaster", Type: "address"},
		{Name: "paymasterData", Type: "bytes"},
		{Name: "deployer", Type: "address"},
		{Name: "deployerData", Type: "bytes"},
		{Name: "executionData", Type: "bytes"},
		{Name: "authorizationData", Type: "bytes"},
	})

	args := abi.Arguments{
		{Type: structThing, Name: "param_one"},
	}

	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := tx.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}

	var authorizations bytes.Buffer
	b := make([]byte, authorizationsSize(tx.AuthorizationData)) // NOTE: may be wrong??
	if err := encodeAuthorizations(tx.AuthorizationData, &authorizations, b); err != nil {
		return nil, err
	}

	record := &types.ABIAccountAbstractTxn{
		Sender:                      *tx.SenderAddress,
		NonceKey:                    tx.NonceKey,
		Nonce:                       uint256.NewInt(tx.Nonce),
		ValidationGasLimit:          uint256.NewInt(tx.ValidationGasLimit),
		PaymasterValidationGasLimit: uint256.NewInt(tx.PaymasterValidationGasLimit),
		PostOpGasLimit:              uint256.NewInt(tx.PostOpGasLimit),
		CallGasLimit:                uint256.NewInt(tx.Gas),
		MaxFeePerGas:                tx.FeeCap,
		MaxPriorityFeePerGas:        tx.Tip,
		BuilderFee:                  tx.BuilderFee,
		Paymaster:                   *paymaster,
		PaymasterData:               tx.PaymasterData,
		Deployer:                    *deployer,
		DeployerData:                tx.DeployerData,
		ExecutionData:               tx.ExecutionData,
		AuthorizationData:           authorizations.Bytes(),
	}
	packed, err := args.Pack(&record)
	return packed, err
}

func (tx *AccountAbstractionTransaction) paymasterFrame(chainID *big.Int) (*Message, error) {
	zeroAddress := common.Address{}
	if tx.Paymaster == nil || bytes.Compare(zeroAddress[:], tx.Paymaster[:]) == 0 {
		return nil, nil
	}

	signingHash := tx.SigningHash(chainID)
	txAbiEncoding, err := tx.AbiEncode()
	if err != nil {
		return nil, err
	}

	validatePaymasterData, err := types.AccountAbstractionABI.Pack("validatePaymasterTransaction", big.NewInt(types.AccountAbstractionABIVersion), signingHash, txAbiEncoding)
	if err != nil {
		return nil, err
	}
	return &Message{
		to:       tx.Paymaster,
		from:     AA_ENTRY_POINT,
		gasLimit: tx.PaymasterValidationGasLimit,
		data:     validatePaymasterData,
	}, nil
}

func (tx *AccountAbstractionTransaction) ExecuteAATransaction(
	paymasterContext []byte,
	validationGasUsed uint64,
	gasPool *core.GasPool,
	evm *vm.EVM,
) (executionStatus uint64, executionReturnData []byte, postOpReturnData []byte, err error) {
	executionStatus = ExecutionStatusSuccess

	// Execution frame
	msg := tx.executionFrame()
	applyRes, err := core.ApplyFrame(evm, msg, gasPool, nil)
	if err != nil {
		return 0, nil, nil, err
	}

	if applyRes.Failed() {
		executionStatus = ExecutionStatusExecutionFailure
	}
	executionReturnData = applyRes.ReturnData

	executionGasPenalty := (tx.Gas - applyRes.UsedGas) * AA_GAS_PENALTY_PCT / 100
	gasUsed := validationGasUsed + applyRes.UsedGas + executionGasPenalty
	gasRefund := capRefund(applyRes.UsedGas+validationGasUsed, gasUsed) // TODO: check correctness, i think this should be moved into statetransition
	finalGasUsed := gasUsed - gasRefund

	// Paymaster post-op frame
	msg, err = tx.paymasterPostOp(paymasterContext, finalGasUsed, !applyRes.Failed())
	if err != nil {
		return 0, nil, nil, err
	}

	applyRes, err = core.ApplyFrame(evm, msg, gasPool, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if applyRes.Failed() {
		if executionStatus == ExecutionStatusExecutionFailure {
			executionStatus = ExecutionStatusExecutionFailure
		} else {
			executionStatus = ExecutionStatusPostOpFailure
		}

		// TODO: cap refund, unused gas penalty

		return 0, nil, nil, errors.New("paymaster post-op failed")
	}
	postOpReturnData = applyRes.ReturnData

	return executionStatus, executionReturnData, postOpReturnData, nil
}

func (tx *AccountAbstractionTransaction) paymasterPostOp(paymasterContext []byte, gasUsed uint64, executionSuccess bool) (*Message, error) {
	postOpData, err := types.AccountAbstractionABI.Pack("postPaymasterTransaction", executionSuccess, big.NewInt(int64(gasUsed)), paymasterContext)
	if err != nil {
		return nil, errors.New("unable to encode postPaymasterTransaction")
	}

	return &Message{
		to:       tx.Paymaster,
		from:     AA_SENDER_CREATOR,
		gasLimit: tx.PostOpGasLimit,
		data:     postOpData,
	}, nil
}

// TODO: get rid of?
func capRefund(getRefund uint64, gasUsed uint64) uint64 {
	refund := gasUsed / params.RefundQuotientEIP3529
	if refund > getRefund {
		return getRefund
	}
	return refund
}

func (tx *AccountAbstractionTransaction) executionFrame() *Message {
	return &Message{
		to:       tx.SenderAddress,
		from:     AA_ENTRY_POINT,
		gasLimit: tx.Gas,
		data:     tx.ExecutionData,
	}
}

func (tx *AccountAbstractionTransaction) InjectAALogs(executionStatus, blockNum uint64, execReturnData, postOpReturnData []byte, ibs *state.IntraBlockState) error {
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
	if executionStatus == ExecutionStatusExecutionFailure || executionStatus == ExecutionStatusExecutionAndPostOpFailure {
		err = injectRIP7560TransactionRevertReasonEvent(tx, execReturnData, blockNum, ibs)
		if err != nil {
			return err
		}
	}
	if executionStatus == ExecutionStatusPostOpFailure || executionStatus == ExecutionStatusExecutionAndPostOpFailure {
		err = injectRIP7560TransactionPostOpRevertReasonEvent(tx, postOpReturnData, blockNum, ibs)
		if err != nil {
			return err
		}
	}
	return nil
}

func injectRIP7560AccountDeployedEvent(
	txn *AccountAbstractionTransaction,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := txn.AbiEncodeRIP7560AccountDeployedEvent()
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
	txn *AccountAbstractionTransaction,
	revertData []byte,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := txn.AbiEncodeRIP7560TransactionRevertReasonEvent(revertData)
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
	txn *AccountAbstractionTransaction,
	revertData []byte,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := txn.AbiEncodeRIP7560TransactionPostOpRevertReasonEvent(revertData)
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
	txn *AccountAbstractionTransaction,
	executionStatus uint64,
	blockNum uint64,
	ibs *state.IntraBlockState,
) error {
	topics, data, err := types.AbiEncodeRIP7560TransactionEvent(txn, executionStatus)
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
	transactionLog := &Log{
		Address:     AA_ENTRY_POINT,
		Topics:      topics,
		Data:        data,
		BlockNumber: blockNumber,
	}
	ibs.AddLog(transactionLog)
	return nil
}

func (tx *AccountAbstractionTransaction) PreTransactionGasCost() (uint64, error) {
	var authorizationsBytes bytes.Buffer
	b := make([]byte, authorizationsSize(tx.AuthorizationData)) // NOTE: may be wrong??
	if err := encodeAuthorizations(tx.AuthorizationData, &authorizationsBytes, b); err != nil {
		return 0, err
	}

	// data should have tx.AuthorizationData, tx.DeployerData, tx.ExecutionData, tx.PaymasterData
	data := make([]byte, len(authorizationsBytes.Bytes())+len(tx.DeployerData)+len(tx.ExecutionData)+len(tx.PaymasterData))
	data = append(data, authorizationsBytes.Bytes()...)
	data = append(data, tx.DeployerData...)
	data = append(data, tx.ExecutionData...)
	data = append(data, tx.PaymasterData...)
	return core.IntrinsicGas(data, tx.AccessList, false, true, true, true, true, uint64(len(tx.AuthorizationData))) // NOTE: should read homestead and 2028 config from chainconfig
}

func performStaticValidation(
	txn *AccountAbstractionTransaction,
	senderCodeSize, paymasterCodeSize, deployerCodeSize int,
) error {
	hasPaymaster := txn.Paymaster != nil
	hasPaymasterData := txn.PaymasterData != nil && len(txn.PaymasterData) != 0
	hasPaymasterGasLimit := txn.PaymasterValidationGasLimit != 0
	hasDeployer := txn.Deployer != nil
	hasDeployerData := txn.DeployerData != nil && len(txn.DeployerData) != 0
	hasCodeSender := senderCodeSize != 0
	hasCodeDeployer := deployerCodeSize != 0

	if !hasDeployer && hasDeployerData {
		return wrapError(
			fmt.Errorf(
				"deployer data of size %d is provided but deployer address is not set",
				len(txn.DeployerData),
			),
		)
	}
	if !hasPaymaster && (hasPaymasterData || hasPaymasterGasLimit) {
		return wrapError(
			fmt.Errorf(
				"paymaster data of size %d (or a gas limit: %d) is provided but paymaster address is not set",
				len(txn.DeployerData),
				txn.PaymasterValidationGasLimit,
			),
		)
	}

	if hasPaymaster {
		if !hasPaymasterGasLimit {
			return wrapError(
				fmt.Errorf(
					"paymaster address  %s is provided but 'paymasterVerificationGasLimit' is zero",
					txn.Paymaster.String(),
				),
			)
		}
		hasCodePaymaster := paymasterCodeSize != 0
		if !hasCodePaymaster {
			return wrapError(
				fmt.Errorf(
					"paymaster address %s is provided but contract has no code deployed",
					txn.Paymaster.String(),
				),
			)
		}
	}

	if hasDeployer {
		if !hasCodeDeployer {
			return wrapError(
				fmt.Errorf(
					"deployer address %s is provided but contract has no code deployed",
					txn.Deployer.String(),
				),
			)
		}
		if hasCodeSender {
			return wrapError(
				fmt.Errorf(
					"sender address %s and deployer address %s are provided but sender is already deployed",
					txn.SenderAddress.String(),
					txn.Deployer.String(),
				))
		}
	}

	preTransactionGasCost, _ := txn.PreTransactionGasCost() // might not be needed: this is checked when we do ApplyMessage
	if preTransactionGasCost > txn.ValidationGasLimit {
		return wrapError(
			fmt.Errorf(
				"insufficient ValidationGasLimit(%d) to cover PreTransactionGasCost(%d)",
				txn.ValidationGasLimit, preTransactionGasCost,
			),
		)
	}

	if !hasDeployer && !hasCodeSender {
		return wrapError(
			fmt.Errorf(
				"account is not deployed and no deployer is specified, account:%s", txn.SenderAddress.String(),
			),
		)
	}

	return nil
}

// ValidationPhaseError is an API error that encompasses an EVM revert with JSON error
// code and a binary data blob.
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

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560AccountDeployedEvent() (topics []common.Hash, data []byte, err error) {
	id := types.AccountAbstractionABI.Events["RIP7560AccountDeployed"].ID
	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := tx.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}
	topics = []common.Hash{id, {}, {}, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(libcommon.LeftPadBytes(paymaster.Bytes()[:], 32))
	topics[3] = [32]byte(libcommon.LeftPadBytes(deployer.Bytes()[:], 32))
	return topics, make([]byte, 0), nil
}

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560TransactionRevertReasonEvent(
	revertData []byte,
) (topics []common.Hash, data []byte, error error) {
	id := types.AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].ID
	inputs := types.AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		tx.NonceKey,
		big.NewInt(int64(tx.Nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	return topics, data, nil
}

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560TransactionPostOpRevertReasonEvent(
	revertData []byte,
) (topics []common.Hash, data []byte, error error) {
	id := types.AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].ID
	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	inputs := types.AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		tx.NonceKey,
		big.NewInt(int64(tx.Nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(libcommon.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}
