package types

import (
	"errors"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
)

type AccountAbstractionBatchHeaderTransaction struct {
	TransactionMisc
	ChainID          *uint256.Int
	TransactionCount uint64

	// Hash fields
	BlockNumber uint64 `rlp:"nil"`
	TxnIndex    uint64 `rlp:"nil"`
}

func (tx *AccountAbstractionBatchHeaderTransaction) SetBlockNumber(blockNumber uint64) {
	tx.BlockNumber = blockNumber
}

func (tx *AccountAbstractionBatchHeaderTransaction) SetTxnIndex(txnIndex uint64) {
	tx.TxnIndex = txnIndex
}

// Transaction interface

func (tx *AccountAbstractionBatchHeaderTransaction) Type() byte {
	return AccountAbstractionBatchHeaderType
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetChainID() *uint256.Int {
	return tx.ChainID
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetNonce() uint64 {
	return 0
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetPrice() *uint256.Int {
	return nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetTip() *uint256.Int {
	return nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetFeeCap() *uint256.Int {
	return nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetBlobHashes() []libcommon.Hash {
	return []libcommon.Hash{}
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetGas() uint64 {
	return 0
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetBlobGas() uint64 {
	return 0
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetValue() *uint256.Int {
	return nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetTo() *libcommon.Address {
	return nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	return Message{}, errors.New("not implemented")
}

func (tx *AccountAbstractionBatchHeaderTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	return nil, errors.New("not implemented")
}

func (tx *AccountAbstractionBatchHeaderTransaction) Hash() libcommon.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}

	hash := prefixedRlpHash(AccountAbstractionTxType, []interface{}{
		tx.ChainID,
		tx.TransactionCount,
		tx.BlockNumber,
		tx.TxnIndex,
	})

	tx.hash.Store(&hash)
	return hash
}

func (tx *AccountAbstractionBatchHeaderTransaction) SigningHash(chainID *big.Int) libcommon.Hash {
	return tx.Hash()
}
func (tx *AccountAbstractionBatchHeaderTransaction) GetData() []byte {
	return []byte{}
}
func (tx *AccountAbstractionBatchHeaderTransaction) GetAccessList() AccessList {
	return AccessList{}
}
func (tx *AccountAbstractionBatchHeaderTransaction) Protected() bool {
	return false
}
func (tx *AccountAbstractionBatchHeaderTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return nil, nil, nil
}

func (tx *AccountAbstractionBatchHeaderTransaction) payloadSize() (payloadSize int) {
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.ChainID)

	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.TransactionCount)

	return
}

func (tx *AccountAbstractionBatchHeaderTransaction) EncodingSize() int {
	payloadSize := tx.payloadSize()
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *AccountAbstractionBatchHeaderTransaction) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, tx)
}

func (tx *AccountAbstractionBatchHeaderTransaction) DecodeRLP(s *rlp.Stream) error {
	b, err := s.Bytes()
	if err != nil {
		return err
	}

	return rlp.DecodeBytes(b, tx)
}

func (tx *AccountAbstractionBatchHeaderTransaction) MarshalBinary(w io.Writer) error {
	return tx.EncodeRLP(w)
}

func (tx *AccountAbstractionBatchHeaderTransaction) Sender(Signer) (libcommon.Address, error) {
	return libcommon.Address{}, errors.New("not implemented")
}

func (tx *AccountAbstractionBatchHeaderTransaction) cachedSender() (libcommon.Address, bool) {
	return libcommon.Address{}, false
}

func (tx *AccountAbstractionBatchHeaderTransaction) GetSender() (libcommon.Address, bool) {
	return libcommon.Address{}, false
}

func (tx *AccountAbstractionBatchHeaderTransaction) SetSender(libcommon.Address) {
	return
}

func (tx *AccountAbstractionBatchHeaderTransaction) IsContractDeploy() bool {
	return false
}

func (tx *AccountAbstractionBatchHeaderTransaction) Unwrap() Transaction {
	return tx
}
