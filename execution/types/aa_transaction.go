package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/fixedgas"
	"github.com/erigontech/erigon/execution/rlp"
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
	GasLimit   uint64
	AccessList AccessList

	SenderAddress               *common.Address
	SenderValidationData        []byte
	ExecutionData               []byte
	Paymaster                   *common.Address
	PaymasterData               []byte
	Deployer                    *common.Address
	DeployerData                []byte
	BuilderFee                  *uint256.Int
	ValidationGasLimit          uint64
	PaymasterValidationGasLimit uint64
	PostOpGasLimit              uint64
	Authorizations              []Authorization

	// RIP-7712 two-dimensional nonce (optional), 192 bits
	NonceKey *uint256.Int
}

func (tx *AccountAbstractionTransaction) GetData() []byte {
	return []byte{}
}

func (tx *AccountAbstractionTransaction) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *AccountAbstractionTransaction) GetAuthorizations() []Authorization {
	return tx.Authorizations
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

func (tx *AccountAbstractionTransaction) GetGasLimit() uint64 {
	return params.TxAAGas + tx.ValidationGasLimit + tx.PaymasterValidationGasLimit + tx.GasLimit + tx.PostOpGasLimit
}

func (tx *AccountAbstractionTransaction) GetTipCap() *uint256.Int {
	return uint256.NewInt(0)
}

func (tx *AccountAbstractionTransaction) GetBlobHashes() []common.Hash {
	return []common.Hash{}
}

func (tx *AccountAbstractionTransaction) GetGas() uint64 {
	return tx.GasLimit
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

func (tx *AccountAbstractionTransaction) Type() byte {
	return AccountAbstractionTxType
}

func (tx *AccountAbstractionTransaction) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	return &Message{
		to:         nil,
		gasPrice:   *tx.FeeCap,
		blobHashes: []common.Hash{},
	}, nil
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
		tx.SenderAddress, tx.SenderValidationData,
		tx.Deployer, tx.DeployerData,
		tx.Paymaster, tx.PaymasterData,
		tx.ExecutionData,
		tx.BuilderFee,
		tx.Tip, tx.FeeCap,
		tx.ValidationGasLimit, tx.PaymasterValidationGasLimit, tx.PostOpGasLimit,
		tx.GasLimit,
		tx.AccessList,
		tx.Authorizations,
	})

	tx.hash.Store(&hash)
	return hash
}

func (tx *AccountAbstractionTransaction) SigningHash(chainID *big.Int) common.Hash {
	hash := prefixedRlpHash(AccountAbstractionTxType, []interface{}{
		chainID,
		tx.NonceKey, tx.Nonce,
		tx.SenderAddress, tx.SenderValidationData,
		tx.Deployer, tx.DeployerData,
		tx.Paymaster, tx.PaymasterData,
		tx.ExecutionData,
		tx.BuilderFee,
		tx.Tip, tx.FeeCap,
		tx.ValidationGasLimit, tx.PaymasterValidationGasLimit, tx.PostOpGasLimit,
		tx.GasLimit,
		tx.AccessList, // authorization data is not included for signing hash
	})

	return hash
}

func (tx *AccountAbstractionTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return new(uint256.Int), new(uint256.Int), new(uint256.Int)
}

func (tx *AccountAbstractionTransaction) payloadSize() (payloadSize, accessListLen, authorizationsLen int) {
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.ChainID)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.NonceKey)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.Nonce)

	payloadSize++
	if tx.SenderAddress != nil {
		payloadSize += 20
	}

	payloadSize += rlp.StringLen(tx.SenderValidationData)

	payloadSize++
	if tx.Deployer != nil {
		payloadSize += 20
	}

	payloadSize += rlp.StringLen(tx.DeployerData)

	payloadSize++
	if tx.Paymaster != nil {
		payloadSize += 20
	}

	payloadSize += rlp.StringLen(tx.PaymasterData)

	payloadSize += rlp.StringLen(tx.ExecutionData)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.BuilderFee)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.Tip)

	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.FeeCap)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.ValidationGasLimit)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.PaymasterValidationGasLimit)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.PostOpGasLimit)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.GasLimit)

	accessListLen = accessListSize(tx.AccessList)
	payloadSize += rlp.ListPrefixLen(accessListLen) + accessListLen

	authorizationsLen = authorizationsSize(tx.Authorizations)
	payloadSize += rlp.ListPrefixLen(authorizationsLen) + authorizationsLen

	return
}

func (tx *AccountAbstractionTransaction) EncodingSize() int {
	payloadSize, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *AccountAbstractionTransaction) EncodeRLP(w io.Writer) error {
	payloadSize, accessListLen, authorizationsLen := tx.payloadSize()
	envelopSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode envelope size
	if err := rlp.EncodeStringSizePrefix(envelopSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = AccountAbstractionTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}

	if err := tx.encodePayload(w, b[:], payloadSize, accessListLen, authorizationsLen); err != nil {
		return err
	}

	return nil
}

func (tx *AccountAbstractionTransaction) encodePayload(w io.Writer, b []byte, payloadSize, accessListLen, authorizationsLen int) error {
	// prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeUint256(tx.ChainID, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeUint256(tx.NonceKey, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeOptionalAddress(tx.SenderAddress, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeString(tx.SenderValidationData, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeOptionalAddress(tx.Deployer, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeString(tx.DeployerData, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeOptionalAddress(tx.Paymaster, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeString(tx.PaymasterData, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeString(tx.ExecutionData, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeUint256(tx.BuilderFee, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeUint256(tx.Tip, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeUint256(tx.FeeCap, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.ValidationGasLimit, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.PaymasterValidationGasLimit, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.PostOpGasLimit, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.GasLimit, w, b); err != nil {
		return err
	}

	// prefix
	if err := rlp.EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(tx.AccessList, w, b); err != nil {
		return err
	}

	// prefix
	if err := rlp.EncodeStructSizePrefix(authorizationsLen, w, b); err != nil {
		return err
	}
	// encode Authorizations
	if err := encodeAuthorizations(tx.Authorizations, w, b); err != nil {
		return err
	}

	return nil
}

func (tx *AccountAbstractionTransaction) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.ChainID = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.NonceKey = new(uint256.Int).SetBytes(b)

	if tx.Nonce, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for SenderAddress: %d", len(b))
	}
	tx.SenderAddress = &common.Address{}
	copy((*tx.SenderAddress)[:], b)

	if tx.SenderValidationData, err = s.Bytes(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}

	if len(b) == 20 {
		tx.Deployer = &common.Address{}
		copy((*tx.Deployer)[:], b)
	} else if len(b) != 0 {
		return fmt.Errorf("wrong size for Deployer: %d", len(b))
	}

	if tx.DeployerData, err = s.Bytes(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}

	if len(b) == 20 {
		tx.Paymaster = &common.Address{}
		copy((*tx.Paymaster)[:], b)
	} else if len(b) != 0 {
		return fmt.Errorf("wrong size for Paymaster: %d", len(b))
	}

	if tx.PaymasterData, err = s.Bytes(); err != nil {
		return err
	}

	if tx.ExecutionData, err = s.Bytes(); err != nil {
		return err
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.BuilderFee = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.Tip = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.FeeCap = new(uint256.Int).SetBytes(b)

	if tx.ValidationGasLimit, err = s.Uint(); err != nil {
		return err
	}

	if tx.PaymasterValidationGasLimit, err = s.Uint(); err != nil {
		return err
	}

	if tx.PostOpGasLimit, err = s.Uint(); err != nil {
		return err
	}

	if tx.GasLimit, err = s.Uint(); err != nil {
		return err
	}

	// decode AccessList
	tx.AccessList = AccessList{}
	if err = decodeAccessList(&tx.AccessList, s); err != nil {
		return err
	}

	// decode authorizations
	tx.Authorizations = make([]Authorization, 0)
	if err = decodeAuthorizations(&tx.Authorizations, s); err != nil {
		return err
	}

	return s.ListEnd()
}

func (tx *AccountAbstractionTransaction) MarshalBinary(w io.Writer) error {
	payloadSize, accessListLen, authorizationsLen := tx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode TxType
	b[0] = AccountAbstractionTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, accessListLen, authorizationsLen); err != nil {
		return err
	}
	return nil
}

func (tx *AccountAbstractionTransaction) PreTransactionGasCost(rules *chain.Rules, hasEIP3860 bool) (uint64, error) {
	// data should have tx.SenderValidationData, tx.DeployerData, tx.ExecutionData, tx.PaymasterData
	data := make([]byte, 0, len(tx.SenderValidationData)+len(tx.DeployerData)+len(tx.ExecutionData)+len(tx.PaymasterData))
	data = append(data, tx.SenderValidationData...)
	data = append(data, tx.DeployerData...)
	data = append(data, tx.ExecutionData...)
	data = append(data, tx.PaymasterData...)
	gas, _, overflow := fixedgas.IntrinsicGas(data, uint64(len(tx.AccessList)), uint64(tx.AccessList.StorageKeys()), false, rules.IsHomestead, rules.IsIstanbul, hasEIP3860, rules.IsPrague, true, uint64(len(tx.Authorizations)))

	if overflow {
		return 0, errors.New("overflow")
	}

	return gas, nil
}

func (tx *AccountAbstractionTransaction) DeployerFrame(rules *chain.Rules, hasEIP3860 bool) *Message {
	intrinsicGas, _ := tx.PreTransactionGasCost(rules, hasEIP3860)
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
		gasLimit: tx.GasLimit,
		data:     tx.ExecutionData,
	}
}

func (tx *AccountAbstractionTransaction) PaymasterPostOp(paymasterContext []byte, gasUsed uint64, executionSuccess bool) (*Message, error) {
	postOpData, err := EncodePostOpFrame(paymasterContext, big.NewInt(int64(gasUsed)), executionSuccess)
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
	if tx.Paymaster == nil || bytes.Equal(zeroAddress[:], tx.Paymaster[:]) {
		return nil, nil
	}

	signingHash := tx.SigningHash(chainID)
	txAbiEncoding, err := tx.AbiEncode()
	if err != nil {
		return nil, err
	}

	validatePaymasterData, err := EncodeTxnForFrame("validatePaymasterTransaction", signingHash, txAbiEncoding)
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

func (tx *AccountAbstractionTransaction) ValidationFrame(chainID *big.Int, deploymentGasUsed uint64, rules *chain.Rules, hasEIP3860 bool) (*Message, error) {
	signingHash := tx.SigningHash(chainID)
	txAbiEncoding, err := tx.AbiEncode()
	if err != nil {
		return nil, err
	}

	validateTransactionData, err := EncodeTxnForFrame("validateTransaction", signingHash, txAbiEncoding)
	if err != nil {
		return nil, err
	}

	intrinsicGas, _ := tx.PreTransactionGasCost(rules, hasEIP3860)
	accountGasLimit := tx.ValidationGasLimit - intrinsicGas - deploymentGasUsed

	return &Message{
		to:       tx.SenderAddress,
		from:     AA_ENTRY_POINT,
		gasLimit: accountGasLimit,
		data:     validateTransactionData,
	}, nil
}

func (tx *AccountAbstractionTransaction) GasPayer() *common.Address {
	if tx.Paymaster != nil && tx.Paymaster.Cmp(common.Address{}) != 0 {
		return tx.Paymaster
	}

	return tx.SenderAddress
}

func (tx *AccountAbstractionTransaction) AbiEncode() ([]byte, error) {
	abiType, _ := abi.NewType("tuple", "tuple", []abi.ArgumentMarshaling{ // internaltype does not matter
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
		{Name: "senderValidationData", Type: "bytes"},
		{Name: "paymaster", Type: "address"},
		{Name: "paymasterData", Type: "bytes"},
		{Name: "deployer", Type: "address"},
		{Name: "deployerData", Type: "bytes"},
		{Name: "executionData", Type: "bytes"}, // TODO: discuss how to pass authorization data to EVM
	})

	args := abi.Arguments{
		{Type: abiType, Name: "param_one"}, // name does not matter
	}

	paymaster := tx.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := tx.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}

	record := &ABIAccountAbstractTxn{
		Sender:                      *tx.SenderAddress,
		NonceKey:                    tx.NonceKey.ToBig(),
		Nonce:                       big.NewInt(int64(tx.Nonce)),
		ValidationGasLimit:          big.NewInt(int64(tx.ValidationGasLimit)),
		PaymasterValidationGasLimit: big.NewInt(int64(tx.PaymasterValidationGasLimit)),
		PostOpGasLimit:              big.NewInt(int64(tx.PostOpGasLimit)),
		CallGasLimit:                big.NewInt(int64(tx.GasLimit)),
		MaxFeePerGas:                tx.FeeCap.ToBig(),
		MaxPriorityFeePerGas:        tx.Tip.ToBig(),
		BuilderFee:                  tx.BuilderFee.ToBig(),
		SenderValidationData:        tx.SenderValidationData,
		Paymaster:                   *paymaster,
		PaymasterData:               tx.PaymasterData,
		Deployer:                    *deployer,
		DeployerData:                tx.DeployerData,
		ExecutionData:               tx.ExecutionData,
	}
	packed, err := args.Pack(&record)
	return packed, err
}

// ABIAccountAbstractTxn an equivalent of a solidity struct only used to encode the 'transaction' parameter
type ABIAccountAbstractTxn struct {
	Sender                      common.Address
	NonceKey                    *big.Int
	Nonce                       *big.Int
	ValidationGasLimit          *big.Int
	PaymasterValidationGasLimit *big.Int
	PostOpGasLimit              *big.Int
	CallGasLimit                *big.Int
	MaxFeePerGas                *big.Int
	MaxPriorityFeePerGas        *big.Int
	BuilderFee                  *big.Int
	Paymaster                   common.Address
	PaymasterData               []byte
	Deployer                    common.Address
	DeployerData                []byte
	ExecutionData               []byte
	SenderValidationData        []byte
}

func FromProto(tx *typesproto.AccountAbstractionTransaction) *AccountAbstractionTransaction {
	if tx == nil {
		return nil
	}

	senderAddress := common.BytesToAddress(tx.SenderAddress)

	var paymasterAddress, deployerAddress *common.Address

	if len(tx.Paymaster) != 0 {
		address := common.BytesToAddress(tx.Paymaster)
		paymasterAddress = &address
	}

	if len(tx.Deployer) != 0 {
		address := common.BytesToAddress(tx.Deployer)
		deployerAddress = &address
	}

	return &AccountAbstractionTransaction{
		Nonce:                       tx.Nonce,
		ChainID:                     uint256.NewInt(0).SetBytes(tx.ChainId),
		Tip:                         uint256.NewInt(0).SetBytes(tx.Tip),
		FeeCap:                      uint256.NewInt(0).SetBytes(tx.FeeCap),
		GasLimit:                    tx.Gas,
		SenderAddress:               &senderAddress,
		SenderValidationData:        tx.SenderValidationData,
		ExecutionData:               tx.ExecutionData,
		Paymaster:                   paymasterAddress,
		PaymasterData:               tx.PaymasterData,
		Deployer:                    deployerAddress,
		DeployerData:                tx.DeployerData,
		BuilderFee:                  uint256.NewInt(0).SetBytes(tx.BuilderFee),
		ValidationGasLimit:          tx.ValidationGasLimit,
		PaymasterValidationGasLimit: tx.PaymasterValidationGasLimit,
		PostOpGasLimit:              tx.PostOpGasLimit,
		NonceKey:                    uint256.NewInt(0).SetBytes(tx.NonceKey),
		Authorizations:              convertProtoAuthorizations(tx.Authorizations),
	}
}

func convertProtoAuthorizations(auths []*typesproto.Authorization) []Authorization {
	goAuths := make([]Authorization, len(auths))
	var r, s, chainID uint256.Int
	for i, auth := range auths {
		r.SetBytes(auth.R) // Convert bytes to uint256
		s.SetBytes(auth.S)
		chainID.SetUint64(auth.ChainId)
		goAuths[i] = Authorization{
			ChainID: chainID,
			Address: common.BytesToAddress(auth.Address),
			Nonce:   auth.Nonce,
			YParity: uint8(auth.YParity),
			R:       r,
			S:       s,
		}
	}

	return goAuths
}
