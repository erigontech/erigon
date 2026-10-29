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

package systemtx

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var nativeSystemAddress = accounts.InternAddress(protocol.NativeSystemAddress)

// unsigned holds what the rollupX v0.1 transaction types share: no
// signature, no nonce, zero fees, the native system address as sender and
// the EIP-2718 encoding computed once.
type unsigned struct {
	types.TransactionMisc

	chainID uint64
	enc     []byte
	hash    common.Hash
}

func (u *unsigned) init(chainID uint64, enc []byte) {
	u.chainID, u.enc, u.hash = chainID, enc, crypto.Keccak256Hash(enc)
}

func (u *unsigned) GetChainID() *uint256.Int                 { return uint256.NewInt(u.chainID) }
func (u *unsigned) GetNonce() uint64                         { return 0 }
func (u *unsigned) GetTipCap() *uint256.Int                  { return new(uint256.Int) }
func (u *unsigned) GetFeeCap() *uint256.Int                  { return new(uint256.Int) }
func (u *unsigned) GetBlobHashes() []common.Hash             { return nil }
func (u *unsigned) GetBlobGas() uint64                       { return 0 }
func (u *unsigned) GetAccessList() types.AccessList          { return nil }
func (u *unsigned) GetAuthorizations() []types.Authorization { return nil }
func (u *unsigned) Protected() bool                          { return true }
func (u *unsigned) IsContractDeploy() bool                   { return false }
func (u *unsigned) Hash() common.Hash                        { return u.hash }
func (u *unsigned) EncodingSize() int                        { return len(u.enc) }

func (u *unsigned) GetEffectiveGasTip(*uint256.Int) uint256.Int { return uint256.Int{} }

func (u *unsigned) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return new(uint256.Int), new(uint256.Int), new(uint256.Int)
}

func (u *unsigned) WithSignature(types.Signer, []byte) (types.Transaction, error) {
	return nil, errUnsigned
}

// SigningHash returns the transaction hash: nothing signs these types.
func (u *unsigned) SigningHash(*uint256.Int) common.Hash { return u.hash }

// Sender returns protocol.NativeSystemAddress for a signer of the
// transaction's chain, or of no chain.
func (u *unsigned) Sender(sg types.Signer) (accounts.Address, error) {
	if id := sg.ChainID(); id != nil && !id.IsZero() && !id.Eq(uint256.NewInt(u.chainID)) {
		return accounts.NilAddress, types.ErrInvalidChainId
	}
	u.SetSender(nativeSystemAddress)
	return nativeSystemAddress, nil
}

// MarshalBinary writes the EIP-2718 encoding.
func (u *unsigned) MarshalBinary(w io.Writer) error {
	_, err := w.Write(u.enc)
	return err
}

// EncodeRLP writes the block-body form: the EIP-2718 encoding as an RLP
// string.
func (u *unsigned) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()
	if err := rlp.EncodeStringPrefix(len(u.enc), w, b[:]); err != nil {
		return err
	}
	_, err := w.Write(u.enc)
	return err
}

// message returns a fee-free message from protocol.NativeSystemAddress of a
// rollupX v0.1 chain. The nonce is neither checked nor, as the type
// registration declares, incremented.
func (u *unsigned) message(txType byte, s types.Signer, rules *chain.Rules, to common.Address, value *uint256.Int, gas uint64, data []byte) (*types.Message, error) {
	if rules == nil || rules.L2Version != config.RollupXV01 {
		return nil, fmt.Errorf("%w: 0x%x outside a rollupX v0.1 chain", types.ErrTxTypeNotSupported, txType)
	}
	from, err := u.Sender(s)
	if err != nil {
		return nil, err
	}
	var zero uint256.Int
	msg := types.NewMessage(from, accounts.InternAddress(to), 0, value, gas, &zero, &zero, &zero, data, nil,
		false /* checkNonce */, false /* checkTransaction */, false /* checkGas */, true /* isFree */, nil)
	msg.SetTxType(txType)
	return msg, nil
}

func zero(values ...hexutil.U256) bool {
	for _, v := range values {
		if !(*uint256.Int)(&v).IsZero() {
			return false
		}
	}
	return true
}

// NativeTransaction is a type-0x45 transaction: a cross-chain call to EEZL2
// under the rollupX v0.1 rule set. Its value is minted.
type NativeTransaction struct {
	unsigned
	body protocol.NativeTx
}

var _ types.Transaction = (*NativeTransaction)(nil)

// NewNative returns the transaction with the given body.
func NewNative(body protocol.NativeTx) (*NativeTransaction, error) {
	enc, err := body.MarshalBinary()
	if err != nil {
		return nil, err
	}
	tx := &NativeTransaction{body: body}
	tx.init(body.ChainID, enc)
	return tx, nil
}

// Body returns the transaction fields.
func (tx *NativeTransaction) Body() protocol.NativeTx { return tx.body }

func (tx *NativeTransaction) Type() byte                { return protocol.NativeTxType }
func (tx *NativeTransaction) GetGasLimit() uint64       { return tx.body.GasLimit }
func (tx *NativeTransaction) GetValue() *uint256.Int    { return &tx.body.Value }
func (tx *NativeTransaction) GetData() []byte           { return tx.body.Input }
func (tx *NativeTransaction) Unwrap() types.Transaction { return tx }

func (tx *NativeTransaction) GetTo() *common.Address {
	to := tx.body.To
	return &to
}

// AsMessage returns the call to EEZL2 with the encoded gas limit, which
// block validation requires to be the gas the block has left at the
// transaction's position, at most protocol.NativeTxGasCap.
func (tx *NativeTransaction) AsMessage(s types.Signer, _ *uint256.Int, rules *chain.Rules) (*types.Message, error) {
	return tx.message(protocol.NativeTxType, s, rules, tx.body.To, &tx.body.Value, tx.body.GasLimit, tx.body.Input)
}

// DecodeRLP decodes the payload list that follows the type byte.
func (tx *NativeTransaction) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}
	body, err := protocol.DecodeNativeTxBody(raw)
	if err != nil {
		return err
	}
	tx.body = *body
	tx.init(body.ChainID, append([]byte{protocol.NativeTxType}, raw...))
	return nil
}

// nativeJSON is the reference client's rendering: the body, a zero nonce
// and zero fee and signature placeholders.
type nativeJSON struct {
	Type       hexutil.Uint64  `json:"type"`
	Hash       common.Hash     `json:"hash"`
	ChainID    *hexutil.Uint64 `json:"chainId"`
	SourceHash *common.Hash    `json:"sourceHash"`
	Nonce      hexutil.Uint64  `json:"nonce"`
	To         *common.Address `json:"to"`
	Value      *hexutil.U256   `json:"value"`
	Input      *hexutil.Bytes  `json:"input"`
	Gas        *hexutil.Uint64 `json:"gas"`
	GasPrice   hexutil.U256    `json:"gasPrice"`
	V          hexutil.U256    `json:"v"`
	R          hexutil.U256    `json:"r"`
	S          hexutil.U256    `json:"s"`
	YParity    hexutil.U256    `json:"yParity"`
}

func (tx *NativeTransaction) MarshalJSON() ([]byte, error) {
	chainID, gas := hexutil.Uint64(tx.body.ChainID), hexutil.Uint64(tx.body.GasLimit)
	input := hexutil.Bytes(tx.body.Input)
	return json.Marshal(nativeJSON{
		Type:       hexutil.Uint64(protocol.NativeTxType),
		Hash:       tx.Hash(),
		ChainID:    &chainID,
		SourceHash: &tx.body.SourceHash,
		To:         tx.GetTo(),
		Value:      (*hexutil.U256)(&tx.body.Value),
		Input:      &input,
		Gas:        &gas,
	})
}

// UnmarshalJSON reads the body fields and rejects nonzero placeholders.
func (tx *NativeTransaction) UnmarshalJSON(input []byte) error {
	var dec nativeJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.ChainID == nil || dec.SourceHash == nil || dec.Gas == nil || dec.To == nil || dec.Value == nil || dec.Input == nil {
		return fmt.Errorf("%w: chainId, sourceHash, gas, to, value and input are required", protocol.ErrNativeTx)
	}
	if dec.Nonce != 0 || !zero(dec.GasPrice, dec.V, dec.R, dec.S, dec.YParity) {
		return fmt.Errorf("%w: nonzero nonce, gas price or signature", protocol.ErrNativeTx)
	}
	body := protocol.NativeTx{
		ChainID:    uint64(*dec.ChainID),
		SourceHash: *dec.SourceHash,
		GasLimit:   uint64(*dec.Gas),
		To:         *dec.To,
		Value:      uint256.Int(*dec.Value),
		Input:      *dec.Input,
	}
	enc, err := body.MarshalBinary()
	if err != nil {
		return err
	}
	tx.body = body
	tx.init(body.ChainID, enc)
	return nil
}

// HistoryTransaction is a type-0x46 transaction: an L1 history update under
// the rollupX v0.1 rule set. It calls protocol.HistoryAddress with the
// import its blocks imply and the gas its block has left, at most
// protocol.HistoryTxGasCap.
type HistoryTransaction struct {
	unsigned
	body protocol.HistoryTx
}

var _ types.Transaction = (*HistoryTransaction)(nil)

// NewHistory returns the transaction with the given body.
func NewHistory(body protocol.HistoryTx) (*HistoryTransaction, error) {
	enc, err := body.MarshalBinary()
	if err != nil {
		return nil, err
	}
	tx := &HistoryTransaction{body: body}
	tx.init(body.ChainID, enc)
	return tx, nil
}

// Body returns the transaction fields.
func (tx *HistoryTransaction) Body() protocol.HistoryTx { return tx.body }

func (tx *HistoryTransaction) Type() byte                { return protocol.HistoryTxType }
func (tx *HistoryTransaction) GetGasLimit() uint64       { return protocol.HistoryTxGasCap }
func (tx *HistoryTransaction) GetValue() *uint256.Int    { return new(uint256.Int) }
func (tx *HistoryTransaction) Unwrap() types.Transaction { return tx }

// GetData returns nothing: the call data is not a transaction field, and
// the reference client renders it empty.
func (tx *HistoryTransaction) GetData() []byte { return nil }

func (tx *HistoryTransaction) GetTo() *common.Address {
	to := protocol.HistoryAddress
	return &to
}

// AsMessage returns the importL1Blocks call, which takes its gas from the
// block.
func (tx *HistoryTransaction) AsMessage(s types.Signer, _ *uint256.Int, rules *chain.Rules) (*types.Message, error) {
	msg, err := tx.message(protocol.HistoryTxType, s, rules, protocol.HistoryAddress, new(uint256.Int), protocol.HistoryTxGasCap, tx.body.Calldata())
	if err != nil {
		return nil, err
	}
	msg.SetGasFromBlock(true)
	return msg, nil
}

// DecodeRLP decodes the payload list that follows the type byte.
func (tx *HistoryTransaction) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}
	body, err := protocol.DecodeHistoryTxBody(raw)
	if err != nil {
		return err
	}
	tx.body = *body
	tx.init(body.ChainID, append([]byte{protocol.HistoryTxType}, raw...))
	return nil
}

type historyBlockJSON struct {
	Number    hexutil.Uint64 `json:"number"`
	BlockHash common.Hash    `json:"blockHash"`
}

// historyJSON is the reference client's rendering: the body, the derived
// target, the gas cap and zero placeholders.
type historyJSON struct {
	Type     hexutil.Uint64     `json:"type"`
	Hash     common.Hash        `json:"hash"`
	Version  *hexutil.Uint64    `json:"version"`
	ChainID  *hexutil.Uint64    `json:"chainId"`
	Blocks   []historyBlockJSON `json:"blocks"`
	Nonce    hexutil.Uint64     `json:"nonce"`
	To       common.Address     `json:"to"`
	Value    hexutil.U256       `json:"value"`
	Input    hexutil.Bytes      `json:"input"`
	Gas      hexutil.Uint64     `json:"gas"`
	GasPrice hexutil.U256       `json:"gasPrice"`
	V        hexutil.U256       `json:"v"`
	R        hexutil.U256       `json:"r"`
	S        hexutil.U256       `json:"s"`
	YParity  hexutil.U256       `json:"yParity"`
}

func (tx *HistoryTransaction) MarshalJSON() ([]byte, error) {
	version, chainID := hexutil.Uint64(1), hexutil.Uint64(tx.body.ChainID)
	blocks := make([]historyBlockJSON, len(tx.body.Blocks))
	for i, b := range tx.body.Blocks {
		blocks[i] = historyBlockJSON{Number: hexutil.Uint64(b.Number), BlockHash: b.Hash}
	}
	return json.Marshal(historyJSON{
		Type:    hexutil.Uint64(protocol.HistoryTxType),
		Hash:    tx.Hash(),
		Version: &version,
		ChainID: &chainID,
		Blocks:  blocks,
		To:      protocol.HistoryAddress,
		Input:   hexutil.Bytes{},
		Gas:     hexutil.Uint64(protocol.HistoryTxGasCap),
	})
}

// UnmarshalJSON reads the body fields and rejects placeholders other than
// those MarshalJSON writes.
func (tx *HistoryTransaction) UnmarshalJSON(input []byte) error {
	var dec historyJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	switch {
	case dec.Version == nil || dec.ChainID == nil:
		return fmt.Errorf("%w: version and chainId are required", protocol.ErrHistoryTx)
	case uint64(*dec.Version) != 1:
		return fmt.Errorf("%w: version %d", protocol.ErrHistoryTx, uint64(*dec.Version))
	case dec.To != (common.Address{}) && dec.To != protocol.HistoryAddress, len(dec.Input) > 0,
		dec.Gas != 0 && uint64(dec.Gas) != protocol.HistoryTxGasCap, dec.Nonce != 0,
		!zero(dec.Value, dec.GasPrice, dec.V, dec.R, dec.S, dec.YParity):
		return fmt.Errorf("%w: placeholder differs from its fixed value", protocol.ErrHistoryTx)
	}
	body := protocol.HistoryTx{ChainID: uint64(*dec.ChainID), Blocks: make([]protocol.HistoryBlock, len(dec.Blocks))}
	for i, b := range dec.Blocks {
		body.Blocks[i] = protocol.HistoryBlock{Number: uint64(b.Number), Hash: b.BlockHash}
	}
	enc, err := body.MarshalBinary()
	if err != nil {
		return err
	}
	tx.body = body
	tx.init(body.ChainID, enc)
	return nil
}
