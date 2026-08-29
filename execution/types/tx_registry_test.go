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

package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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

func unregisterTxType(id byte) {
	txTypeRegistryMu.Lock()
	defer txTypeRegistryMu.Unlock()
	delete(txTypeRegistry, id)
}

func registerFakeTxType(t *testing.T) {
	t.Helper()
	if _, ok := registeredTxType(fakeRegisteredTxType); ok {
		return
	}
	RegisterTxType(fakeRegisteredTxType, TxTypeSpec{
		New:                    func() Transaction { return &fakeRegisteredTx{} },
		StandardReceiptPayload: true,
		UnmarshalJSON:          unmarshalFakeRegisteredTxJSON,
		Sender: func(txn Transaction, _ Signer) (accounts.Address, error) {
			return txn.(*fakeRegisteredTx).sender, nil
		},
	})
	t.Cleanup(func() { unregisterTxType(fakeRegisteredTxType) })
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

func TestRegisteredTxTypeRejectsTruncatedBlockTransaction(t *testing.T) {
	registerFakeTxType(t)

	data := buildBlockWithTruncatedTypedTx(t, fakeRegisteredTxType)
	s := rlp.NewBytesStream(data)
	defer rlp.PutStream(s)

	var block Block
	if err := block.DecodeRLP(s); err == nil {
		t.Fatalf("expected error decoding block with truncated registered transaction, got nil (len(txs)=%d)", len(block.transactions))
	}
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

type fakeSenderlessTx struct{ fakeRegisteredTx }

func (f *fakeSenderlessTx) Type() byte { return 0x7c }

func TestRegisteredTxTypeWithoutSenderFacetUnsupported(t *testing.T) {
	RegisterTxType(0x7c, TxTypeSpec{New: func() Transaction { return &fakeSenderlessTx{} }})
	t.Cleanup(func() { unregisterTxType(0x7c) })

	tx := &fakeSenderlessTx{}
	_, err := Signer{}.SenderWithContext(nil, tx)
	require.ErrorIs(t, err, ErrTxTypeNotSupported)
}

func TestRegisteredTxTypeSignatureValuesDecodeSig(t *testing.T) {
	registerFakeTxType(t)

	sig := make([]byte, 65)
	for i := range 64 {
		sig[i] = byte(i + 1)
	}
	sig[64] = 1

	sg := *LatestSignerForChainID(uint256.NewInt(1))
	r, s, v, err := sg.SignatureValues(&fakeRegisteredTx{}, sig)
	require.NoError(t, err)
	require.Equal(t, new(uint256.Int).SetBytes(sig[:32]), r)
	require.Equal(t, new(uint256.Int).SetBytes(sig[32:64]), s)
	require.Equal(t, uint256.NewInt(1), v)
}

func TestRegisteredTxTypeJSONTypeAliasing(t *testing.T) {
	registerFakeTxType(t)

	_, err := UnmarshalTransactionFromJSON([]byte(`{"type":"0x17e","nonce":"0x1"}`))
	require.Error(t, err, "an out-of-range type value must not alias onto a registered byte id")
}

func TestJSONBuiltinTypeAliasing(t *testing.T) {
	// byte(0x100) == 0x00 would alias an out-of-range type onto LegacyTxType;
	// it must be rejected as unknown rather than silently decoded as legacy.
	_, err := UnmarshalTransactionFromJSON([]byte(`{"type":"0x100","nonce":"0x1"}`))
	require.ErrorContains(t, err, "unknown transaction type")
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

func fakeRegisteredReceipt() *Receipt {
	r := &Receipt{
		Type:              fakeRegisteredTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 42,
		Logs: []*Log{{
			Address: common.BytesToAddress([]byte{0x11}),
			Topics:  []common.Hash{common.HexToHash("dead")},
			Data:    []byte{0x01, 0x00, 0xff},
		}},
	}
	r.Bloom = CreateBloom(Receipts{r})
	return r
}

func TestRegisteredTxTypeReceiptRoundTrip(t *testing.T) {
	registerFakeTxType(t)

	want := fakeRegisteredReceipt()
	binary, err := want.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, byte(fakeRegisteredTxType), binary[0])

	got := new(Receipt)
	require.NoError(t, got.UnmarshalBinary(binary))
	require.Equal(t, want.Type, got.Type)
	require.Equal(t, want.Status, got.Status)
	require.Equal(t, want.CumulativeGasUsed, got.CumulativeGasUsed)
	require.Equal(t, want.Bloom, got.Bloom)
	require.Equal(t, want.Logs, got.Logs)

	var buf bytes.Buffer
	require.NoError(t, want.EncodeRLP(&buf))
	streamed := new(Receipt)
	require.NoError(t, rlp.DecodeBytes(buf.Bytes(), streamed))
	require.Equal(t, want.Type, streamed.Type)
	require.Equal(t, want.CumulativeGasUsed, streamed.CumulativeGasUsed)
	require.Equal(t, want.Logs, streamed.Logs)

	stored, err := rlp.EncodeToBytes((*ReceiptForStorage)(want))
	require.NoError(t, err)
	unstored := new(ReceiptForStorage)
	require.NoError(t, rlp.DecodeBytes(stored, unstored))
	require.Equal(t, want.Type, unstored.Type)
	require.Equal(t, want.CumulativeGasUsed, unstored.CumulativeGasUsed)
}

func TestRegisteredTxTypeReceiptRoot(t *testing.T) {
	registerFakeTxType(t)

	r := fakeRegisteredReceipt()
	binary, err := r.MarshalBinary()
	require.NoError(t, err)

	var buf bytes.Buffer
	Receipts{r}.EncodeIndex(0, &buf)
	require.Equal(t, binary, buf.Bytes(), "EncodeIndex must agree with MarshalBinary")

	// Golden root over the single 0x7e-prefixed leaf.
	want := common.HexToHash("0x10d0be5b17612ce56aca9e438311c8c76484f24346b5c911857a522aacec5d20")
	require.Equal(t, want, DeriveSha(Receipts{r}))
}

func TestUnregisteredReceiptTypeRejected(t *testing.T) {
	const unknownType = 0x7f

	payload, err := rlp.EncodeToBytes(&receiptRLP{[]byte{1}, 42, Bloom{}, nil})
	require.NoError(t, err)
	binary := append([]byte{unknownType}, payload...)

	require.ErrorIs(t, new(Receipt).UnmarshalBinary(binary), ErrTxTypeNotSupported)

	enveloped, err := rlp.EncodeToBytes(binary)
	require.NoError(t, err)
	require.ErrorIs(t, rlp.DecodeBytes(enveloped, new(Receipt)), ErrTxTypeNotSupported)

	_, err = (&Receipt{Type: unknownType}).MarshalBinary()
	require.ErrorIs(t, err, ErrTxTypeNotSupported)

	// EncodeIndex runs under DeriveSha on the block path, so it contributes no
	// leaf rather than panicking; the block is rejected on the root mismatch.
	var derived bytes.Buffer
	Receipts{{Type: unknownType}}.EncodeIndex(0, &derived)
	require.Empty(t, derived.Bytes())

	_, err = rlp.EncodeToBytes(&ReceiptForStorage{Type: unknownType, Status: ReceiptStatusSuccessful})
	require.ErrorContains(t, err, "invalid receipt type")

	// Bypass the encoder to stand in for bytes an older binary already wrote.
	stored, err := rlp.EncodeToBytes(&storedReceiptRLP{Type: unknownType, PostStateOrStatus: []byte{1}})
	require.NoError(t, err)
	require.ErrorContains(t, rlp.DecodeBytes(stored, new(ReceiptForStorage)), "invalid receipt type")
}

// A type whose ReceiptPayload is not the built-in one — OP's deposit receipts
// add two consensus fields — must be refused rather than encoded as standard.
func TestRegisteredTxTypeWithoutStandardReceiptPayload(t *testing.T) {
	const customReceiptType = 0x7a

	RegisterTxType(customReceiptType, TxTypeSpec{New: func() Transaction { return &fakeRegisteredTx{} }})
	t.Cleanup(func() { unregisterTxType(customReceiptType) })

	r := &Receipt{Type: customReceiptType, Status: ReceiptStatusSuccessful, CumulativeGasUsed: 1}

	// Both directions, or the encode side emits bytes this package's own
	// decoder rejects.
	_, err := r.MarshalBinary()
	require.ErrorIs(t, err, ErrTxTypeNotSupported)
	require.ErrorIs(t, r.EncodeRLP(new(bytes.Buffer)), ErrTxTypeNotSupported)
	require.ErrorIs(t, r.EncodeRLP69(new(bytes.Buffer)), ErrTxTypeNotSupported)

	payload, err := rlp.EncodeToBytes(&receiptRLP{r.statusEncoding(), r.CumulativeGasUsed, r.Bloom, r.Logs})
	require.NoError(t, err)
	require.ErrorIs(t, new(Receipt).UnmarshalBinary(append([]byte{customReceiptType}, payload...)), ErrTxTypeNotSupported)

	var derived bytes.Buffer
	Receipts{r}.EncodeIndex(0, &derived)
	require.Empty(t, derived.Bytes(), "a type that never claimed the standard payload must not get one")

	_, err = rlp.EncodeToBytes((*ReceiptForStorage)(r))
	require.ErrorContains(t, err, "invalid receipt type")
}
