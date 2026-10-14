// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.
// Package systemtx registers the EEZ system transaction, type 0x76, with the
// transaction pipeline of execution/types.
package systemtx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	errUnsigned   = errors.New("eez system transaction carries no signature")
	systemAddress = accounts.InternAddress(protocol.SystemAddress)
)

var registerOnce sync.Once

// Register adds type 0x76 to the transaction pipeline.
func Register() {
	registerOnce.Do(func() {
		types.RegisterTxType(protocol.SystemTxType, types.TxTypeSpec{
			New: func() types.Transaction { return new(Transaction) },
			UnmarshalJSON: func(input []byte) (types.Transaction, error) {
				tx := new(Transaction)
				if err := tx.UnmarshalJSON(input); err != nil {
					return nil, err
				}
				return tx, nil
			},
			Sender: func(txn types.Transaction, sg types.Signer) (accounts.Address, error) {
				return txn.Sender(sg)
			},
			StandardReceiptPayload: true,
		})
		execprotocol.RegisterMessageHook(protocol.SystemTxType, mintHook{})
	})
}

// Transaction is a type-0x76 transaction. It has no signature and no gas
// fields: the sender is protocol.SystemAddress, the gas limit
// protocol.SystemTxGasLimit and every fee zero.
type Transaction struct {
	types.TransactionMisc

	body protocol.SystemTx
	hash atomic.Pointer[common.Hash]
}

var _ types.Transaction = (*Transaction)(nil)

// New returns the transaction with the given body, which must target EEZL2.
func New(body protocol.SystemTx) (*Transaction, error) {
	if body.To != protocol.EEZL2Address {
		return nil, fmt.Errorf("%w: target %v is not EEZL2", protocol.ErrSystemTx, body.To)
	}
	return &Transaction{body: body}, nil
}

// Body returns the transaction fields.
func (tx *Transaction) Body() protocol.SystemTx { return tx.body }

func (tx *Transaction) Type() byte                               { return protocol.SystemTxType }
func (tx *Transaction) GetChainID() *uint256.Int                 { return uint256.NewInt(tx.body.ChainID) }
func (tx *Transaction) GetNonce() uint64                         { return tx.body.Nonce }
func (tx *Transaction) GetTipCap() *uint256.Int                  { return new(uint256.Int) }
func (tx *Transaction) GetFeeCap() *uint256.Int                  { return new(uint256.Int) }
func (tx *Transaction) GetBlobHashes() []common.Hash             { return nil }
func (tx *Transaction) GetGasLimit() uint64                      { return protocol.SystemTxGasLimit }
func (tx *Transaction) GetBlobGas() uint64                       { return 0 }
func (tx *Transaction) GetValue() *uint256.Int                   { return &tx.body.Value }
func (tx *Transaction) GetData() []byte                          { return tx.body.Input }
func (tx *Transaction) GetAccessList() types.AccessList          { return nil }
func (tx *Transaction) GetAuthorizations() []types.Authorization { return nil }
func (tx *Transaction) Protected() bool                          { return true }
func (tx *Transaction) IsContractDeploy() bool                   { return false }
func (tx *Transaction) Unwrap() types.Transaction                { return tx }

func (tx *Transaction) GetTo() *common.Address {
	to := tx.body.To
	return &to
}

func (tx *Transaction) GetEffectiveGasTip(*uint256.Int) uint256.Int { return uint256.Int{} }

func (tx *Transaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return new(uint256.Int), new(uint256.Int), new(uint256.Int)
}

func (tx *Transaction) WithSignature(types.Signer, []byte) (types.Transaction, error) {
	return nil, errUnsigned
}

// SigningHash returns the transaction hash: nothing signs a system
// transaction.
func (tx *Transaction) SigningHash(*uint256.Int) common.Hash { return tx.Hash() }

// Sender returns protocol.SystemAddress for a signer of the transaction's
// chain, or of no chain.
func (tx *Transaction) Sender(sg types.Signer) (accounts.Address, error) {
	if id := sg.ChainID(); id != nil && !id.IsZero() && !id.Eq(uint256.NewInt(tx.body.ChainID)) {
		return accounts.NilAddress, types.ErrInvalidChainId
	}
	if from, ok := tx.GetSender(); ok {
		return from, nil
	}
	tx.SetSender(systemAddress)
	return systemAddress, nil
}

// AsMessage returns a fee-free message from protocol.SystemAddress whose
// value is minted before execution. The nonce is checked; the EIP-3607 sender
// check and the EIP-7825 gas cap are not. Outside an EEZ chain the type is
// not supported.
func (tx *Transaction) AsMessage(s types.Signer, _ *uint256.Int, rules *chain.Rules) (*types.Message, error) {
	if rules == nil || rules.L2Version < config.ProtocolVersion {
		return nil, fmt.Errorf("%w: 0x%x outside an EEZ chain", types.ErrTxTypeNotSupported, protocol.SystemTxType)
	}
	from, err := tx.Sender(s)
	if err != nil {
		return nil, err
	}
	var zero uint256.Int
	msg := types.NewMessage(from, accounts.InternAddress(tx.body.To), tx.body.Nonce, &tx.body.Value,
		protocol.SystemTxGasLimit, &zero, &zero, &zero, tx.body.Input, nil,
		true /* checkNonce */, false /* checkTransaction */, false /* checkGas */, true /* isFree */, nil)
	msg.SetTxType(protocol.SystemTxType)
	return msg, nil
}

func (tx *Transaction) Hash() common.Hash {
	if h := tx.hash.Load(); h != nil {
		return *h
	}
	hasher := crypto.NewKeccakState()
	defer crypto.ReturnToPool(hasher)
	if err := tx.MarshalBinary(hasher); err != nil {
		panic(err)
	}
	var h common.Hash
	_, _ = hasher.Read(h[:])
	tx.hash.Store(&h)
	return h
}

func (tx *Transaction) payloadSize() int {
	return rlp.U64Len(tx.body.ChainID) + rlp.U64Len(tx.body.Nonce) + 1 + length.Addr +
		rlp.Uint256Len(tx.body.Value) + rlp.StringLen(tx.body.Input)
}

// EncodingSize returns the length of the EIP-2718 encoding.
func (tx *Transaction) EncodingSize() int {
	payloadSize := tx.payloadSize()
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

// MarshalBinary writes the EIP-2718 encoding 0x76 ‖ rlp(body).
func (tx *Transaction) MarshalBinary(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()
	b[0] = protocol.SystemTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	return tx.encodePayload(w, b[:])
}

// EncodeRLP writes the block-body form: the EIP-2718 encoding as an RLP
// string.
func (tx *Transaction) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()
	if err := rlp.EncodeStringPrefix(tx.EncodingSize(), w, b[:]); err != nil {
		return err
	}
	b[0] = protocol.SystemTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	return tx.encodePayload(w, b[:])
}

func (tx *Transaction) encodePayload(w io.Writer, b []byte) error {
	if err := rlp.EncodeListPrefix(tx.payloadSize(), w, b); err != nil {
		return err
	}
	if err := rlp.EncodeU64(tx.body.ChainID, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeU64(tx.body.Nonce, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeString(tx.body.To[:], w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(tx.body.Value, w, b); err != nil {
		return err
	}
	return rlp.EncodeString(tx.body.Input, w, b)
}

// DecodeRLP decodes the payload list that follows the type byte.
func (tx *Transaction) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	var (
		body protocol.SystemTx
		err  error
	)
	if body.ChainID, err = s.Uint64(); err != nil {
		return err
	}
	if body.Nonce, err = s.Uint64(); err != nil {
		return err
	}
	if err := s.ReadBytes(body.To[:]); err != nil {
		return err
	}
	if err := s.ReadUint256(&body.Value); err != nil {
		return err
	}
	if body.Input, err = s.Bytes(); err != nil {
		return err
	}
	if err := s.ListEnd(); err != nil {
		return err
	}
	if body.To != protocol.EEZL2Address {
		return fmt.Errorf("%w: target %v is not EEZL2", protocol.ErrSystemTx, body.To)
	}
	tx.body = body
	return nil
}

// txJSON is the reference client's rendering: the body, the fixed gas
// fields and zero signature placeholders.
type txJSON struct {
	Type     hexutil.Uint64  `json:"type"`
	Hash     common.Hash     `json:"hash"`
	ChainID  *hexutil.Uint64 `json:"chainId"`
	Nonce    *hexutil.Uint64 `json:"nonce"`
	To       *common.Address `json:"to"`
	Value    *hexutil.U256   `json:"value"`
	Input    *hexutil.Bytes  `json:"input"`
	Gas      hexutil.Uint64  `json:"gas"`
	GasPrice hexutil.U256    `json:"gasPrice"`
	V        hexutil.U256    `json:"v"`
	R        hexutil.U256    `json:"r"`
	S        hexutil.U256    `json:"s"`
}

func (tx *Transaction) MarshalJSON() ([]byte, error) {
	chainID, nonce := hexutil.Uint64(tx.body.ChainID), hexutil.Uint64(tx.body.Nonce)
	input := hexutil.Bytes(tx.body.Input)
	return json.Marshal(txJSON{
		Type:    hexutil.Uint64(protocol.SystemTxType),
		Hash:    tx.Hash(),
		ChainID: &chainID,
		Nonce:   &nonce,
		To:      tx.GetTo(),
		Value:   (*hexutil.U256)(&tx.body.Value),
		Input:   &input,
		Gas:     hexutil.Uint64(protocol.SystemTxGasLimit),
	})
}

// UnmarshalJSON reads the body fields; like the reference client it ignores
// the fixed gas fields and signature placeholders.
func (tx *Transaction) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.ChainID == nil || dec.Nonce == nil || dec.To == nil || dec.Value == nil || dec.Input == nil {
		return fmt.Errorf("%w: chainId, nonce, to, value and input are required", protocol.ErrSystemTx)
	}
	body := protocol.SystemTx{
		ChainID: uint64(*dec.ChainID),
		Nonce:   uint64(*dec.Nonce),
		To:      *dec.To,
		Value:   uint256.Int(*dec.Value),
		Input:   *dec.Input,
	}
	if body.To != protocol.EEZL2Address {
		return fmt.Errorf("%w: target %v is not EEZL2", protocol.ErrSystemTx, body.To)
	}
	tx.body = body
	return nil
}
