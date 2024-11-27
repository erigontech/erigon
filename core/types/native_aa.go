package types

import (
	"bytes"
	"errors"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	rlp2 "github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/rlp"
)

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
	hash := prefixedRlpHash(DynamicFeeTxType, []interface{}{
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
	return tx.Hash()
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
