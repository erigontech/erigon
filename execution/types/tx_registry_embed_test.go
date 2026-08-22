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

// Package types_test pins from outside the package that embedding
// TransactionMisc is sufficient for an external transaction type to satisfy
// the Transaction interface (cachedSender is unexported) and ride the
// registry end to end.
package types_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const externalTxType = 0x7b

type externalTx struct {
	types.TransactionMisc

	Nonce uint64
	from  accounts.Address
}

var _ types.Transaction = (*externalTx)(nil)

func (tx *externalTx) Type() byte                               { return externalTxType }
func (tx *externalTx) GetChainID() *uint256.Int                 { return uint256.NewInt(0) }
func (tx *externalTx) GetNonce() uint64                         { return tx.Nonce }
func (tx *externalTx) GetTipCap() *uint256.Int                  { return uint256.NewInt(0) }
func (tx *externalTx) GetFeeCap() *uint256.Int                  { return uint256.NewInt(0) }
func (tx *externalTx) GetBlobHashes() []common.Hash             { return nil }
func (tx *externalTx) GetGasLimit() uint64                      { return 21000 }
func (tx *externalTx) GetBlobGas() uint64                       { return 0 }
func (tx *externalTx) GetValue() *uint256.Int                   { return uint256.NewInt(0) }
func (tx *externalTx) GetTo() *common.Address                   { return nil }
func (tx *externalTx) GetData() []byte                          { return nil }
func (tx *externalTx) GetAccessList() types.AccessList          { return nil }
func (tx *externalTx) GetAuthorizations() []types.Authorization { return nil }
func (tx *externalTx) Protected() bool                          { return true }
func (tx *externalTx) IsContractDeploy() bool                   { return false }
func (tx *externalTx) Unwrap() types.Transaction                { return tx }

func (tx *externalTx) GetEffectiveGasTip(*uint256.Int) uint256.Int { return uint256.Int{} }

func (tx *externalTx) AsMessage(types.Signer, *uint256.Int, *chain.Rules) (*types.Message, error) {
	return nil, nil
}

func (tx *externalTx) WithSignature(types.Signer, []byte) (types.Transaction, error) {
	return tx, nil
}

func (tx *externalTx) Hash() common.Hash                    { return types.RlpHash([]any{tx.Nonce}) }
func (tx *externalTx) SigningHash(*uint256.Int) common.Hash { return tx.Hash() }

func (tx *externalTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	z := uint256.NewInt(0)
	return z, z, z
}

func (tx *externalTx) EncodingSize() int { return rlp.U64Len(tx.Nonce) }

func (tx *externalTx) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()
	if err := rlp.EncodeListPrefix(rlp.U64Len(tx.Nonce), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeU64(tx.Nonce, w, b[:])
}

func (tx *externalTx) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	var err error
	if tx.Nonce, err = s.Uint64(); err != nil {
		return err
	}
	return s.ListEnd()
}

func (tx *externalTx) MarshalBinary(w io.Writer) error {
	if _, err := w.Write([]byte{externalTxType}); err != nil {
		return err
	}
	return tx.EncodeRLP(w)
}

func (tx *externalTx) Sender(types.Signer) (accounts.Address, error) {
	if from, ok := tx.GetSender(); ok {
		return from, nil
	}
	tx.SetSender(tx.from)
	return tx.from, nil
}

func TestExternalPackageTxTypeEndToEnd(t *testing.T) {
	want := accounts.InternAddress(common.HexToAddress("0xabcd"))
	types.RegisterTxType(externalTxType, types.TxTypeSpec{
		New: func() types.Transaction { return &externalTx{} },
		Sender: func(txn types.Transaction, _ types.Signer) (accounts.Address, error) {
			return txn.(*externalTx).Sender(types.Signer{})
		},
	})
	t.Cleanup(func() { types.UnregisterTxTypeForTesting(externalTxType) })

	var buf bytes.Buffer
	require.NoError(t, (&externalTx{Nonce: 7, from: want}).MarshalBinary(&buf))

	decoded, err := types.UnmarshalTransactionFromBinary(buf.Bytes(), false)
	require.NoError(t, err)
	require.Equal(t, uint64(7), decoded.GetNonce())

	decoded.SetSender(want)
	got, err := types.Signer{}.SenderWithContext(nil, decoded)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
