package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	emath "github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/accounts/abi"
)

const (
	ExecutionStatusSuccess                   = uint64(0)
	ExecutionStatusExecutionFailure          = uint64(1)
	ExecutionStatusPostOpFailure             = uint64(2)
	ExecutionStatusExecutionAndPostOpFailure = uint64(3)
)

const AA_GAS_PENALTY_PCT = 10

var AA_ENTRY_POINT = common.HexToAddress("0x0000000000000000000000000000000000007560")
var AA_SENDER_CREATOR = common.HexToAddress("0x00000000000000000000000000000000ffff7560")

type AccountAbstractionTransaction struct {
	TransactionMisc
	Nonce      uint64
	ChainID    *uint256.Int
	Tip        *uint256.Int
	FeeCap     *uint256.Int
	Gas        uint64
	AccessList AccessList

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

func (tx *AccountAbstractionTransaction) GetData() []byte {
	return []byte{}
}

func (tx *AccountAbstractionTransaction) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *AccountAbstractionTransaction) Protected() bool {
	return true
}

func (tx *AccountAbstractionTransaction) Sender(signer Signer) (common.Address, error) {
	return *tx.SenderAddress, nil
}

func (tx *AccountAbstractionTransaction) cachedSender() (common.Address, bool) {
	return *tx.SenderAddress, true
}

func (tx *AccountAbstractionTransaction) GetSender() (common.Address, bool) {
	return *tx.SenderAddress, true
}

func (tx *AccountAbstractionTransaction) SetSender(address common.Address) {
	return
}

func (tx *AccountAbstractionTransaction) IsContractDeploy() bool {
	return false
}

func (tx *AccountAbstractionTransaction) Unwrap() Transaction {
	return tx
}

func (tx *AccountAbstractionTransaction) GetChainID() *uint256.Int {
	return tx.ChainID
}

func (tx *AccountAbstractionTransaction) GetNonce() uint64 {
	return tx.Nonce
}
func (tx *AccountAbstractionTransaction) GetPrice() *uint256.Int {
	return tx.Tip
}

func (tx *AccountAbstractionTransaction) GetTip() *uint256.Int {
	return tx.Tip
}

func (tx *AccountAbstractionTransaction) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetTip()
	}
	gasFeeCap := tx.GetFeeCap()
	// return 0 because effectiveFee cant be < 0
	if gasFeeCap.Lt(baseFee) {
		return uint256.NewInt(0)
	}
	effectiveFee := new(uint256.Int).Sub(gasFeeCap, baseFee)
	if tx.GetTip().Lt(effectiveFee) {
		return tx.GetTip()
	} else {
		return effectiveFee
	}
}

func (tx *AccountAbstractionTransaction) GetFeeCap() *uint256.Int {
	return tx.FeeCap
}

func (tx *AccountAbstractionTransaction) GetBlobHashes() []common.Hash {
	return []common.Hash{}
}

func (tx *AccountAbstractionTransaction) GetGas() uint64 {
	return tx.Gas
}

func (tx *AccountAbstractionTransaction) GetBlobGas() uint64 {
	return 0
}

func (tx *AccountAbstractionTransaction) GetValue() *uint256.Int {
	return uint256.NewInt(0)
}

func (tx *AccountAbstractionTransaction) GetTo() *common.Address {
	return nil
}

func (tx *AccountAbstractionTransaction) copy() *AccountAbstractionTransaction {
	cpy := &AccountAbstractionTransaction{
		Nonce:                       tx.Nonce,
		ChainID:                     tx.ChainID,
		Tip:                         tx.Tip,
		FeeCap:                      tx.FeeCap,
		Gas:                         tx.Gas,
		AccessList:                  tx.AccessList,
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

func (tx *AccountAbstractionTransaction) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	return Message{}, errors.New("do not use")
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
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.ChainID)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.Tip)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.FeeCap)

	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.Gas)
	payloadSize += gasLen

	accessListLen = accessListSize(tx.AccessList)
	payloadSize += rlp.ListPrefixLen(accessListLen) + accessListLen

	payloadSize++
	if tx.SenderAddress != nil {
		payloadSize += 20
	}

	authorizationsLen := authorizationsSize(tx.AuthorizationData)
	payloadSize += rlp.ListPrefixLen(authorizationsLen) + authorizationsLen

	payloadSize++
	payloadSize += rlp.StringLen(tx.ExecutionData)

	payloadSize++
	if tx.Paymaster != nil {
		payloadSize += 20
	}

	payloadSize++
	payloadSize += rlp.StringLen(tx.PaymasterData)

	payloadSize++
	if tx.Deployer != nil {
		payloadSize += 20
	}

	payloadSize++
	payloadSize += rlp.StringLen(tx.DeployerData)

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
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
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
	return IntrinsicGas(data, tx.AccessList, false, true, true, true, true, uint64(len(tx.AuthorizationData))) // NOTE: should read homestead and 2028 config from chainconfig
}

// TODO: remove this, it is a duplicate from core package
func IntrinsicGas(data []byte, accessList AccessList, isContractCreation bool, isHomestead, isEIP2028, isEIP3860, isAATxn bool, authorizationsLen uint64) (uint64, error) {
	// Zero and non-zero bytes are priced differently
	dataLen := uint64(len(data))
	dataNonZeroLen := uint64(0)
	for _, byt := range data {
		if byt != 0 {
			dataNonZeroLen++
		}
	}

	gas := CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen, accessList, isContractCreation, isHomestead, isEIP2028, isEIP3860, isAATxn)
	return gas, nil
}

// TODO: remove this, it is a duplicate from txpoolcfg package
func CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen uint64, accessList AccessList, isContractCreation, isHomestead, isEIP2028, isShanghai, isAATxn bool) uint64 {
	// Set the starting gas for the raw transaction
	var gas uint64
	if isContractCreation && isHomestead {
		gas = fixedgas.TxGasContractCreation
	} else if isAATxn {
		gas = fixedgas.TxAAGas
	} else {
		gas = fixedgas.TxGas
	}
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := dataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := fixedgas.TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = fixedgas.TxDataNonZeroGasEIP2028
		}

		product, overflow := emath.SafeMul(nz, nonZeroGas)
		if overflow {
			return 0
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0
		}

		z := dataLen - nz

		product, overflow = emath.SafeMul(z, fixedgas.TxDataZeroGas)
		if overflow {
			return 0
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0
		}

		if isContractCreation && isShanghai {
			numWords := toWordSize(dataLen)
			product, overflow = emath.SafeMul(numWords, fixedgas.InitCodeWordGas)
			if overflow {
				return 0
			}
			gas, overflow = emath.SafeAdd(gas, product)
			if overflow {
				return 0
			}
		}
	}
	if accessList != nil {
		product, overflow := emath.SafeMul(uint64(len(accessList)), fixedgas.TxAccessListAddressGas)
		if overflow {
			return 0
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0
		}

		product, overflow = emath.SafeMul(uint64(accessList.StorageKeys()), fixedgas.TxAccessListStorageKeyGas)
		if overflow {
			return 0
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0
		}
	}

	// Add the cost of authorizations
	product, overflow := emath.SafeMul(authorizationsLen, fixedgas.PerEmptyAccountCost)
	if overflow {
		return 0
	}

	gas, overflow = emath.SafeAdd(gas, product)
	if overflow {
		return 0
	}

	return gas
}

// TODO: remove this, it is a duplicate from txpoolcfg package
// toWordSize returns the ceiled word size required for memory expansion.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}

func (tx *AccountAbstractionTransaction) DeployerFrame() *Message {
	intrinsicGas, _ := tx.PreTransactionGasCost()
	deployerGasLimit := tx.ValidationGasLimit - intrinsicGas
	return &Message{
		to:       tx.Deployer,
		from:     AA_SENDER_CREATOR,
		gasLimit: deployerGasLimit,
		data:     tx.DeployerData,
	}
}

func (tx *AccountAbstractionTransaction) ExecutionFrame() *Message {
	return &Message{
		to:       tx.SenderAddress,
		from:     AA_ENTRY_POINT,
		gasLimit: tx.Gas,
		data:     tx.ExecutionData,
	}
}

func (tx *AccountAbstractionTransaction) PaymasterPostOp(paymasterContext []byte, gasUsed uint64, executionSuccess bool) (*Message, error) {
	postOpData, err := AccountAbstractionABI.Pack("postPaymasterTransaction", executionSuccess, big.NewInt(int64(gasUsed)), paymasterContext)
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

func (tx *AccountAbstractionTransaction) PaymasterFrame(chainID *big.Int) (*Message, error) {
	zeroAddress := common.Address{}
	if tx.Paymaster == nil || bytes.Compare(zeroAddress[:], tx.Paymaster[:]) == 0 {
		return nil, nil
	}

	signingHash := tx.SigningHash(chainID)
	txAbiEncoding, err := tx.AbiEncode()
	if err != nil {
		return nil, err
	}

	validatePaymasterData, err := AccountAbstractionABI.Pack("validatePaymasterTransaction", big.NewInt(AccountAbstractionABIVersion), signingHash, txAbiEncoding)
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

	record := &ABIAccountAbstractTxn{
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

func (tx *AccountAbstractionTransaction) ValidationFrame(chainID *big.Int, deploymentUsedGas uint64) (*Message, error) {
	signingHash := tx.SigningHash(chainID)
	txAbiEncoding, err := tx.AbiEncode()
	if err != nil {
		return nil, err
	}

	validateTransactionData, err := AccountAbstractionABI.Pack("validateTransaction", big.NewInt(AccountAbstractionABIVersion), signingHash, txAbiEncoding)
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

// ABIAccountAbstractTxn an equivalent of a solidity struct only used to encode the 'transaction' parameter
type ABIAccountAbstractTxn struct {
	// NOTE: these were big.Int and were changed to uint256
	Sender                      common.Address
	NonceKey                    *uint256.Int
	Nonce                       *uint256.Int
	ValidationGasLimit          *uint256.Int
	PaymasterValidationGasLimit *uint256.Int
	PostOpGasLimit              *uint256.Int
	CallGasLimit                *uint256.Int
	MaxFeePerGas                *uint256.Int
	MaxPriorityFeePerGas        *uint256.Int
	BuilderFee                  *uint256.Int
	Paymaster                   common.Address
	PaymasterData               []byte
	Deployer                    common.Address
	DeployerData                []byte
	ExecutionData               []byte
	AuthorizationData           []byte
}

const AccountAbstractionABIJSON = `[{"type":"function","name":"validateTransaction","inputs":[{"name":"version","type":"uint256"},{"name":"txHash","type":"bytes32"},{"name":"transaction","type":"bytes"}]},{"type":"function","name":"validatePaymasterTransaction","inputs":[{"name":"version","type":"uint256"},{"name":"txHash","type":"bytes32"},{"name":"transaction","type":"bytes"}]},{"type":"function","name":"postPaymasterTransaction","inputs":[{"name":"success","type":"bool"},{"name":"actualGasCost","type":"uint256"},{"name":"context","type":"bytes"}]},{"type":"function","name":"acceptAccount","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"}]},{"type":"function","name":"acceptPaymaster","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"},{"name":"context","type":"bytes"}]},{"type":"function","name":"sigFailAccount","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"}]},{"type":"function","name":"sigFailPaymaster","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"},{"name":"context","type":"bytes"}]},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bool","name":"executionStatus","type":"uint256"}],"name":"RIP7560TransactionEvent","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"revertReason","type":"bytes"}],"name":"RIP7560TransactionRevertReason","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"revertReason","type":"bytes"}],"name":"RIP7560TransactionPostOpRevertReason","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":true,"internalType":"address","name":"deployer","type":"address"}],"name":"RIP7560AccountDeployed","type":"event"}]`
const AccountAbstractionABIVersion = 0
const PaymasterMaxContextSize = 65536

var AccountAbstractionABI, _ = abi.JSON(strings.NewReader(AccountAbstractionABIJSON))

func decodeMethodParamsToInterface(output interface{}, methodName string, input []byte) error {
	m, err := AccountAbstractionABI.MethodById(input)
	if err != nil {
		return fmt.Errorf("unable to decode %s: %w", methodName, err)
	}
	if methodName != m.Name {
		return fmt.Errorf("unable to decode %s: got wrong method %s", methodName, m.Name)
	}
	params, err := m.Inputs.Unpack(input[4:])
	if err != nil {
		return fmt.Errorf("unable to decode %s: %w", methodName, err)
	}
	err = m.Inputs.Copy(output, params)
	if err != nil {
		return fmt.Errorf("unable to decode %s: %v", methodName, err)
	}
	return nil
}

type AcceptAccountData struct {
	ValidAfter *uint256.Int
	ValidUntil *uint256.Int
}

type AcceptPaymasterData struct {
	ValidAfter *uint256.Int
	ValidUntil *uint256.Int
	Context    []byte
}

func AbiDecodeAcceptAccount(input []byte, allowSigFail bool) (*AcceptAccountData, error) {
	acceptAccountData := &AcceptAccountData{}
	err := decodeMethodParamsToInterface(acceptAccountData, "acceptAccount", input)
	if err != nil && allowSigFail {
		err = decodeMethodParamsToInterface(acceptAccountData, "sigFailAccount", input)
	}
	if err != nil {
		return nil, err
	}
	return acceptAccountData, nil
}

func AbiDecodeAcceptPaymaster(input []byte, allowSigFail bool) (*AcceptPaymasterData, error) {
	acceptPaymasterData := &AcceptPaymasterData{}
	err := decodeMethodParamsToInterface(acceptPaymasterData, "acceptPaymaster", input)
	if err != nil && allowSigFail {
		err = decodeMethodParamsToInterface(acceptPaymasterData, "sigFailPaymaster", input)
	}
	if err != nil {
		return nil, err
	}
	if len(acceptPaymasterData.Context) > PaymasterMaxContextSize {
		return nil, errors.New("paymaster return data: context too large")
	}
	return acceptPaymasterData, err
}

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560TransactionEvent(
	executionStatus uint64,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionEvent"].ID
	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := tx.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}
	inputs := AccountAbstractionABI.Events["RIP7560TransactionEvent"].Inputs
	data, error = inputs.NonIndexed().Pack(
		tx.NonceKey,
		big.NewInt(int64(tx.Nonce)),
		big.NewInt(int64(executionStatus)),
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(common.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560AccountDeployedEvent() (topics []common.Hash, data []byte, err error) {
	id := AccountAbstractionABI.Events["RIP7560AccountDeployed"].ID
	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := tx.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}
	topics = []common.Hash{id, {}, {}, {}}
	topics[1] = [32]byte(common.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes()[:], 32))
	topics[3] = [32]byte(common.LeftPadBytes(deployer.Bytes()[:], 32))
	return topics, make([]byte, 0), nil
}

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560TransactionRevertReasonEvent(
	revertData []byte,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].ID
	inputs := AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		tx.NonceKey,
		big.NewInt(int64(tx.Nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}}
	topics[1] = [32]byte(common.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	return topics, data, nil
}

func (tx *AccountAbstractionTransaction) AbiEncodeRIP7560TransactionPostOpRevertReasonEvent(
	revertData []byte,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].ID
	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	inputs := AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		tx.NonceKey,
		big.NewInt(int64(tx.Nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(common.LeftPadBytes(tx.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}
