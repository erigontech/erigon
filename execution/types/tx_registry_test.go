// Copyright 2024 The Erigon Authors
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

package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const fakeRegisteredTxType = 0x7e

// fakeRegisteredTx is a minimal Transaction implementation used to prove the
// registry dispatch works for a type this package knows nothing about. It
// embeds TransactionMisc to exercise the sender-cache trio an out-of-package type
// would otherwise be unable to implement.
type fakeRegisteredTx struct {
	TransactionMisc

	Nonce  uint64
	Data   []byte
	sender accounts.Address
}

func (tx *fakeRegisteredTx) Type() byte                         { return fakeRegisteredTxType }
func (tx *fakeRegisteredTx) GetChainID() *uint256.Int           { return uint256.NewInt(0) }
func (tx *fakeRegisteredTx) GetNonce() uint64                   { return tx.Nonce }
func (tx *fakeRegisteredTx) GetTipCap() *uint256.Int            { return uint256.NewInt(0) }
func (tx *fakeRegisteredTx) GetFeeCap() *uint256.Int            { return uint256.NewInt(0) }
func (tx *fakeRegisteredTx) GetBlobHashes() []common.Hash       { return nil }
func (tx *fakeRegisteredTx) GetGasLimit() uint64                { return 21000 }
func (tx *fakeRegisteredTx) GetBlobGas() uint64                 { return 0 }
func (tx *fakeRegisteredTx) GetValue() *uint256.Int             { return uint256.NewInt(0) }
func (tx *fakeRegisteredTx) GetTo() *common.Address             { return nil }
func (tx *fakeRegisteredTx) GetData() []byte                    { return tx.Data }
func (tx *fakeRegisteredTx) GetAccessList() AccessList          { return nil }
func (tx *fakeRegisteredTx) GetAuthorizations() []Authorization { return nil }
func (tx *fakeRegisteredTx) Protected() bool                    { return true }
func (tx *fakeRegisteredTx) IsContractDeploy() bool             { return false }
func (tx *fakeRegisteredTx) Unwrap() Transaction                { return tx }

func (tx *fakeRegisteredTx) GetEffectiveGasTip(baseFee *uint256.Int) uint256.Int {
	return uint256.Int{}
}

func (tx *fakeRegisteredTx) AsMessage(Signer, *uint256.Int, *chain.Rules) (*Message, error) {
	return nil, nil
}

func (tx *fakeRegisteredTx) WithSignature(Signer, []byte) (Transaction, error) {
	return tx, nil
}

func (tx *fakeRegisteredTx) Hash() common.Hash {
	return RlpHash([]any{tx.Nonce, tx.Data})
}

func (tx *fakeRegisteredTx) SigningHash(*uint256.Int) common.Hash {
	return tx.Hash()
}

func (tx *fakeRegisteredTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	z := uint256.NewInt(0)
	return z, z, z
}

func (tx *fakeRegisteredTx) EncodingSize() int {
	return rlp.U64Len(tx.Nonce) + rlp.StringLen(tx.Data)
}

func (tx *fakeRegisteredTx) encodePayload(w io.Writer) error {
	payloadSize := rlp.U64Len(tx.Nonce) + rlp.StringLen(tx.Data)
	b := rlp.NewEncodingBuf()
	defer b.Release()
	if err := rlp.EncodeListPrefix(payloadSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeU64(tx.Nonce, w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeString(tx.Data, w, b[:])
}

func (tx *fakeRegisteredTx) EncodeRLP(w io.Writer) error {
	return tx.encodePayload(w)
}

func (tx *fakeRegisteredTx) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	var err error
	if tx.Nonce, err = s.Uint64(); err != nil {
		return err
	}
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	return s.ListEnd()
}

func (tx *fakeRegisteredTx) MarshalBinary(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()
	b[0] = fakeRegisteredTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	return tx.encodePayload(w)
}

func (tx *fakeRegisteredTx) Sender(Signer) (accounts.Address, error) {
	if from, ok := tx.GetSender(); ok {
		return from, nil
	}
	tx.SetSender(tx.sender)
	return tx.sender, nil
}

type fakeRegisteredTxJSON struct {
	Type  hexutil.Uint64 `json:"type"`
	Nonce hexutil.Uint64 `json:"nonce"`
	Data  hexutil.Bytes  `json:"input"`
}

func (tx *fakeRegisteredTx) MarshalJSON() ([]byte, error) {
	return json.Marshal(fakeRegisteredTxJSON{
		Type:  hexutil.Uint64(fakeRegisteredTxType),
		Nonce: hexutil.Uint64(tx.Nonce),
		Data:  tx.Data,
	})
}

func unmarshalFakeRegisteredTxJSON(input []byte) (Transaction, error) {
	var dec fakeRegisteredTxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return nil, err
	}
	return &fakeRegisteredTx{Nonce: uint64(dec.Nonce), Data: dec.Data}, nil
}

var registerFakeTxTypeOnce sync.Once

func registerFakeTxType(t *testing.T) {
	t.Helper()
	registerFakeTxTypeOnce.Do(func() {
		RegisterTxType(fakeRegisteredTxType, TxTypeSpec{
			New:           func() Transaction { return &fakeRegisteredTx{} },
			UnmarshalJSON: unmarshalFakeRegisteredTxJSON,
		})
	})
}

func TestRegisteredTxTypeBinaryRoundTrip(t *testing.T) {
	registerFakeTxType(t)

	want := &fakeRegisteredTx{Nonce: 7, Data: []byte{0xde, 0xad, 0xbe, 0xef}}
	var buf bytes.Buffer
	require.NoError(t, want.MarshalBinary(&buf))

	got, err := UnmarshalTransactionFromBinary(buf.Bytes(), false)
	require.NoError(t, err)
	gotFake, ok := got.(*fakeRegisteredTx)
	require.True(t, ok)
	require.Equal(t, want.Nonce, gotFake.Nonce)
	require.Equal(t, want.Data, gotFake.Data)
}

func TestRegisteredTxTypeJSONRoundTrip(t *testing.T) {
	registerFakeTxType(t)

	want := &fakeRegisteredTx{Nonce: 9, Data: []byte{0x01, 0x02}}
	encoded, err := json.Marshal(want)
	require.NoError(t, err)

	got, err := UnmarshalTransactionFromJSON(encoded)
	require.NoError(t, err)
	gotFake, ok := got.(*fakeRegisteredTx)
	require.True(t, ok)
	require.Equal(t, want.Nonce, gotFake.Nonce)
	require.Equal(t, want.Data, gotFake.Data)
}

func TestRegisteredTxTypeSenderResolution(t *testing.T) {
	registerFakeTxType(t)

	want := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000001234"))
	tx := &fakeRegisteredTx{Nonce: 1, sender: want}

	signer := Signer{}
	got, err := signer.SenderWithContext(nil, tx)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestRegisterTxTypeCollisions(t *testing.T) {
	builtinSpec := TxTypeSpec{New: func() Transaction { return &LegacyTx{} }}

	require.Panics(t, func() {
		RegisterTxType(LegacyTxType, builtinSpec)
	})
	require.Panics(t, func() {
		RegisterTxType(AccountAbstractionTxType, builtinSpec)
	})

	registerFakeTxType(t)
	require.Panics(t, func() {
		RegisterTxType(fakeRegisteredTxType, builtinSpec)
	})

	require.Panics(t, func() {
		RegisterTxType(0x80, builtinSpec)
	})
}

func TestUnregisteredTxTypeStillUnsupported(t *testing.T) {
	const unknownType = 0x7f

	_, err := UnmarshalTransactionFromBinary([]byte{unknownType, 0x01}, false)
	require.True(t, errors.Is(err, ErrTxTypeNotSupported))

	_, err = UnmarshalTransactionFromJSON([]byte(`{"type":"0x7f"}`))
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrTxTypeNotSupported))
}

func TestRegisteredTxTypeNilJSONDecoder(t *testing.T) {
	RegisterTxType(0x7d, TxTypeSpec{New: func() Transaction { return &fakeRegisteredTx{} }})
	t.Cleanup(func() { unregisterTxType(0x7d) })

	_, err := UnmarshalTransactionFromJSON([]byte(`{"type":"0x7d"}`))
	require.Error(t, err)
}
