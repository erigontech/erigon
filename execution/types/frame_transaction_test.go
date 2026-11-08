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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestFrameTransactionEncoding(t *testing.T) {
	t.Parallel()
	sender := common.HexToAddress("0x1234567890123456789012345678901234567890")
	payload, err := rlp.EncodeToBytes([]any{
		uint64(1),
		[]uint64{1, 2},
		uint64(7), sender,
		[]any{[]any{uint64(1), uint64(3), []byte{}, []uint64{100, 0}, uint64(0), []byte{}}},
		[]any{},
		[]uint64{2, 20, 0},
		[]common.Hash{},
	})
	require.NoError(t, err)
	encoded := append([]byte{0x06}, payload...)
	txn, err := DecodeTransaction(encoded)
	require.NoError(t, err)
	require.Equal(t, byte(0x06), txn.Type())
	require.Equal(t, uint256.NewInt(1), txn.GetChainID())
	require.Equal(t, uint64(7), txn.GetNonce())
	actualSender, err := txn.Sender(*LatestSignerForChainID(uint256.NewInt(1)))
	require.NoError(t, err)
	require.Equal(t, accounts.InternAddress(sender), actualSender)
	require.Equal(t, uint256.NewInt(2), txn.GetTipCap())
	require.Equal(t, uint256.NewInt(20), txn.GetFeeCap())
	var binary bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&binary))
	require.Equal(t, encoded, binary.Bytes())
	require.Equal(t, len(encoded), txn.EncodingSize())
	require.NoError(t, checkTxnShape(encoded))
	var enveloped bytes.Buffer
	require.NoError(t, txn.EncodeRLP(&enveloped))
	decoded, err := DecodeTransaction(enveloped.Bytes())
	require.NoError(t, err)
	require.Equal(t, txn.Hash(), decoded.Hash())
}

func TestFrameTransactionNonceKeys(t *testing.T) {
	for name, keys := range map[string][]uint256.Int{
		"empty":                 nil,
		"too many":              make([]uint256.Int, 17),
		"duplicate":             {{1}, {1}},
		"descending":            {{2}, {1}},
		"zero with another key": {{}, {1}},
	} {
		t.Run(name, func(t *testing.T) {
			tx := &FrameTransaction{NonceKeys: keys, Frames: []Frame{{}}}
			require.Error(t, tx.Validate())
			var data bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&data))
			_, err := DecodeTransaction(data.Bytes())
			require.Error(t, err)

			tx.BlobVersionedHashes = []common.Hash{{1}}
			wrapped := &FrameTxWrapper{FrameTransaction: tx, WrapperVersion: 1, Blobs: Blobs{{}}, Commitments: BlobKzgs{{}}, Proofs: make(KZGProofs, 128)}
			data.Reset()
			require.NoError(t, wrapped.MarshalBinaryWrapped(&data))
			_, err = DecodeWrappedTransaction(data.Bytes())
			require.Error(t, err, "wrapped frames must enforce the same nonce-key rules")
		})
	}
}

func TestFrameTransactionConstraints(t *testing.T) {
	t.Parallel()
	expiry := common.HexToAddress("0x81413f0cF12e9b6a49B1D0439E081c577D57FfFf")
	tests := map[string]func(*FrameTransaction){
		"chain ID":                  func(tx *FrameTransaction) { tx.ChainID.SetUint64(2) },
		"empty frames":              func(tx *FrameTransaction) { tx.Frames = nil },
		"too many frames":           func(tx *FrameTransaction) { tx.Frames = make([]Frame, 65) },
		"mode":                      func(tx *FrameTransaction) { tx.Frames[0].Mode = 3 },
		"flags":                     func(tx *FrameTransaction) { tx.Frames[0].Flags = 8 },
		"value in verify":           func(tx *FrameTransaction) { tx.Frames[0].Value.SetUint64(1) },
		"execution approval target": func(tx *FrameTransaction) { tx.Frames[0].Target = &expiry },
		"atomic verify":             func(tx *FrameTransaction) { tx.Frames[0].Flags = 4; tx.Frames = append(tx.Frames, Frame{}) },
		"atomic last":               func(tx *FrameTransaction) { tx.Frames = []Frame{{Flags: 4}} },
		"atomic next verify":        func(tx *FrameTransaction) { tx.Frames = []Frame{{Flags: 4}, {Mode: 1}} },
		"atomic approval":           func(tx *FrameTransaction) { tx.Frames = []Frame{{Flags: 4}, {Flags: 1}} },
		"gas overflow":              func(tx *FrameTransaction) { tx.Frames[0].Limits.State = math.MaxUint64 },
		"execution cap":             func(tx *FrameTransaction) { tx.Frames[0].Limits.Execution = 16_777_216 },
		"floor cap":                 func(tx *FrameTransaction) { tx.Frames[0].Data = make([]byte, 262_144) },
		"blob fee without blobs":    func(tx *FrameTransaction) { tx.Fees.BlobFeeCap.SetUint64(1) },
		"blob hash version":         func(tx *FrameTransaction) { tx.BlobVersionedHashes = []common.Hash{{2}} },
		"expiry data":               func(tx *FrameTransaction) { tx.Frames = []Frame{{Mode: 1, Target: &expiry}} },
		"expiry scope": func(tx *FrameTransaction) {
			tx.Frames = []Frame{{Mode: 1, Target: &expiry, Flags: 1, Data: make([]byte, 8)}}
		},
		"expiry state": func(tx *FrameTransaction) {
			tx.Frames = []Frame{{Mode: 1, Target: &expiry, Data: make([]byte, 8), Limits: mdgas.MdGas{State: 1}}}
		},
		"duplicate expiry": func(tx *FrameTransaction) {
			tx.Frames = []Frame{{Mode: 1, Target: &expiry, Data: make([]byte, 8)}, {Mode: 1, Target: &expiry, Data: make([]byte, 8)}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			tx := FrameTransaction{NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), Frames: []Frame{{Mode: 1, Flags: 3, Limits: mdgas.MdGas{Execution: 100}}}}
			rules := &chain.Rules{IsBogota: true, ChainID: uint256.NewInt(1)}
			_, err := tx.AsMessage(Signer{}, nil, rules)
			require.NoError(t, err)
			mutate(&tx)
			_, err = tx.AsMessage(Signer{}, nil, rules)
			require.Error(t, err)
		})
	}
}

func signedFrameTransaction(t testing.TB, scheme uint8) (*FrameTransaction, []byte) {
	t.Helper()
	tx := &FrameTransaction{NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), Frames: []Frame{{}}, Signatures: []FrameSignature{{Scheme: scheme}}}
	var highS []byte
	if scheme == FrameSignatureSecp256k1 {
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		tx.SenderAddress = crypto.PubkeyToAddress(key.PublicKey)
		hash := tx.SigningHash(nil)
		sig, err := crypto.Sign(hash[:], key)
		require.NoError(t, err)
		tx.Signatures[0].Signature = append([]byte{sig[64]}, sig[:64]...)
		highS = bytes.Clone(tx.Signatures[0].Signature)
		new(big.Int).Sub(key.Curve.Params().N, new(big.Int).SetBytes(sig[32:64])).FillBytes(highS[33:])
	} else {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		pub := make([]byte, 64)
		key.X.FillBytes(pub[:32])
		key.Y.FillBytes(pub[32:])
		tx.SenderAddress = common.BytesToAddress(crypto.Keccak256(pub)[12:])
		hash := tx.SigningHash(nil)
		r, s, err := ecdsa.Sign(rand.Reader, key, hash[:])
		require.NoError(t, err)
		n := key.Curve.Params().N
		if s.Cmp(new(big.Int).Rsh(new(big.Int).Set(n), 1)) > 0 {
			s.Sub(n, s)
		}
		sig := make([]byte, 128)
		r.FillBytes(sig[:32])
		s.FillBytes(sig[32:64])
		copy(sig[64:], pub)
		tx.Signatures[0].Signature = sig
		highS = bytes.Clone(sig)
		new(big.Int).Sub(n, s).FillBytes(highS[32:64])
	}
	return tx, highS
}

func TestFrameTransactionSignatureValidation(t *testing.T) {
	t.Parallel()
	rules := &chain.Rules{IsBogota: true, ChainID: uint256.NewInt(1)}
	for _, scheme := range []uint8{FrameSignatureSecp256k1, FrameSignatureP256} {
		t.Run(string(rune('0'+scheme)), func(t *testing.T) {
			tx, highS := signedFrameTransaction(t, scheme)
			tx.Hash()
			_, err := tx.AsMessage(Signer{}, nil, rules)
			require.NoError(t, err)
			valid := tx.Signatures[0].Signature
			tx.Signatures[0].Signature = highS
			_, err = tx.AsMessage(Signer{}, nil, rules)
			require.Error(t, err, "high-s signatures must fail")
			tx.Signatures[0].Signature = valid
			_, err = tx.AsMessage(Signer{}, nil, rules)
			require.NoError(t, err)
		})
	}
}

func TestFrameTransactionSignatureValidationAfterMutation(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*FrameTransaction){
		"nonce":   func(tx *FrameTransaction) { tx.Nonce++ },
		"data":    func(tx *FrameTransaction) { tx.Frames[0].Data = []byte{1} },
		"sender":  func(tx *FrameTransaction) { tx.SenderAddress[0] ^= 1 },
		"signer":  func(tx *FrameTransaction) { tx.Signatures[0].Signer = make([]byte, 20) },
		"message": func(tx *FrameTransaction) { tx.Signatures[0].Message = bytes.Repeat([]byte{1}, 32) },
	} {
		t.Run(name, func(t *testing.T) {
			tx, _ := signedFrameTransaction(t, FrameSignatureSecp256k1)
			tx.Hash()
			require.NoError(t, tx.Validate())
			mutate(tx)
			require.ErrorContains(t, tx.Validate(), "invalid frame signature 0")
		})
	}
}

func TestFrameTransactionConcurrentValidation(t *testing.T) {
	for _, scheme := range []uint8{FrameSignatureSecp256k1, FrameSignatureP256} {
		t.Run(string(rune('0'+scheme)), func(t *testing.T) {
			tx, _ := signedFrameTransaction(t, scheme)
			for range 8 {
				t.Run("reader", func(t *testing.T) {
					t.Parallel()
					for range 8 {
						require.NoError(t, tx.Validate())
						_, err := tx.Sender(*LatestSignerForChainID(&tx.ChainID))
						require.NoError(t, err)
						_, err = tx.AsMessage(Signer{}, nil, &chain.Rules{IsBogota: true, ChainID: &tx.ChainID})
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestFrameTransactionMalformedSignature(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		sig  FrameSignature
		want string
	}{
		"scheme":           {FrameSignature{Scheme: 3}, "unknown scheme"},
		"signer length":    {FrameSignature{Signer: []byte{1}}, "invalid signer length"},
		"arbitrary signer": {FrameSignature{Signer: make([]byte, 20)}, "arbitrary signature has signer"},
		"message length":   {FrameSignature{Message: []byte{1}}, "invalid message length"},
		"zero message":     {FrameSignature{Message: make([]byte, 32)}, "zero message digest"},
		"secp length":      {FrameSignature{Scheme: 1}, "invalid signature length"},
		"p256 length":      {FrameSignature{Scheme: 2}, "invalid signature length"},
		"secp scalars":     {FrameSignature{Scheme: 1, Signature: make([]byte, 65)}, "signature verification failed"},
		"p256 scalars":     {FrameSignature{Scheme: 2, Signature: make([]byte, 128)}, "signature verification failed"},
	} {
		t.Run(name, func(t *testing.T) {
			tx := FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []Frame{{}}, Signatures: []FrameSignature{test.sig}}
			_, err := tx.AsMessage(Signer{}, nil, &chain.Rules{IsBogota: true, ChainID: new(uint256.Int)})
			require.EqualError(t, err, "invalid frame signature 0: "+test.want)
		})
	}
}

func TestFrameTransactionAsMessage(t *testing.T) {
	t.Parallel()
	tx := FrameTransaction{
		NonceKeys: []uint256.Int{{}},
		ChainID:   *uint256.NewInt(1), SenderAddress: common.HexToAddress("0x1234"),
		Frames: []Frame{{Mode: FrameVerify, Flags: FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}}},
		Fees:   FrameFees{TipCap: *uint256.NewInt(2), FeeCap: *uint256.NewInt(20)},
	}
	rules := &chain.Rules{IsBogota: true, ChainID: uint256.NewInt(1)}
	msg, err := tx.AsMessage(Signer{}, uint256.NewInt(10), rules)
	require.NoError(t, err)
	require.Equal(t, accounts.InternAddress(tx.SenderAddress), msg.From())
	require.Equal(t, uint256.NewInt(12), msg.GasPrice())
	require.Equal(t, tx.GetGasLimit(), msg.Gas())
	rules.IsBogota = false
	_, err = tx.AsMessage(Signer{}, uint256.NewInt(10), rules)
	require.ErrorIs(t, err, ErrTxTypeNotSupported)
}

func TestFrameTransactionExpiryRequiresExplicitTarget(t *testing.T) {
	t.Parallel()
	tx := FrameTransaction{NonceKeys: []uint256.Int{{}}, SenderAddress: FrameExpiryVerifier, Frames: []Frame{{Mode: FrameVerify, Flags: FrameApproveBoth}}}
	require.NoError(t, tx.Validate())
}

func TestFrameTransactionGasLimit(t *testing.T) {
	t.Parallel()
	t.Run("keyed nonce access", func(t *testing.T) {
		tx := FrameTransaction{NonceKeys: []uint256.Int{{1}, {2}}, Nonce: 128, Frames: []Frame{{}}}
		intrinsic, floor, limits, err := tx.GasLimits()
		require.NoError(t, err)
		require.Equal(t, uint64(12_000+475+5*16+2*12_100), intrinsic)
		require.Equal(t, uint64(12_000+475+5*64+2*12_100), floor)
		require.Equal(t, floor, limits.Execution)
	})
	t.Run("intrinsic", func(t *testing.T) {
		tx := FrameTransaction{
			NonceKeys:  []uint256.Int{{}},
			Frames:     []Frame{{Limits: mdgas.MdGas{Execution: 10_000, State: 1_000}, Data: []byte{0, 1}}},
			Signatures: []FrameSignature{{Signature: []byte{0, 2}}},
		}
		require.Equal(t, uint64(12_000+475+48+40+100+10_000+1_000), tx.GetGasLimit())
	})
	t.Run("calldata floor", func(t *testing.T) {
		tx := FrameTransaction{
			NonceKeys: []uint256.Int{{}},
			Frames:    []Frame{{Limits: mdgas.MdGas{Execution: 100, State: 1_000}, Data: make([]byte, 100)}},
		}
		require.Equal(t, uint64(12_000+475+103*64+1_000), tx.GetGasLimit())
	})
	t.Run("value transfer", func(t *testing.T) {
		target := common.HexToAddress("0x1234")
		tx := FrameTransaction{
			NonceKeys: []uint256.Int{{}},
			Frames:    []Frame{{Mode: 2, Target: &target, Value: *uint256.NewInt(1)}},
		}
		require.Equal(t, uint64(12_000+475+192+6_000), tx.GetGasLimit())
		tx.Frames[0].Target = nil
		require.Equal(t, uint64(12_000+475+192), tx.GetGasLimit())
	})
}

func TestFrameTransactionSigningHash(t *testing.T) {
	t.Parallel()
	sender := common.HexToAddress("0x1234567890123456789012345678901234567890")
	explicitMessage := bytes.Repeat([]byte{1}, 32)
	fields := []any{
		uint64(1),
		[]uint64{1, 2},
		uint64(7), sender,
		[]any{[]any{uint64(1), uint64(3), []byte{}, []uint64{100, 0}, uint64(0), []byte{}}},
		[]any{
			[]any{uint64(0), []byte{}, []byte{}, []byte{1, 2, 3}},
			[]any{uint64(0), []byte{}, explicitMessage, []byte{4, 5}},
		},
		[]uint64{2, 20, 0},
		[]common.Hash{},
	}
	payload, err := rlp.EncodeToBytes(fields)
	require.NoError(t, err)
	encoded := append([]byte{0x06}, payload...)
	txn, err := DecodeTransaction(encoded)
	require.NoError(t, err)
	fields[5] = []any{
		[]any{uint64(0), []byte{}, []byte{}, []byte{}},
		[]any{uint64(0), []byte{}, explicitMessage, []byte{4, 5}},
	}
	elided, err := rlp.EncodeToBytes(fields)
	require.NoError(t, err)
	expected := crypto.Keccak256Hash(append([]byte{0x06}, elided...))
	require.Equal(t, expected, txn.SigningHash(txn.GetChainID()))
	require.Equal(t, crypto.Keccak256Hash(encoded), txn.Hash())
	var binary bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&binary))
	require.Equal(t, encoded, binary.Bytes())
}

func TestFrameTransactionSigner(t *testing.T) {
	t.Parallel()
	tx := &FrameTransaction{NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: common.HexToAddress("0x1234"), Frames: []Frame{{}}}
	for _, tc := range []struct {
		name   string
		signer Signer
		err    error
	}{
		{"valid", *LatestSignerForChainID(uint256.NewInt(1)), nil},
		{"wrong chain", *LatestSignerForChainID(uint256.NewInt(2)), ErrInvalidChainId},
		{"before fork", *MakeSigner(&chain.Config{ChainID: uint256.NewInt(1)}, 0, 0), ErrTxTypeNotSupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender, err := tc.signer.Sender(tx)
			require.ErrorIs(t, err, tc.err)
			fromTx, err := tx.Sender(tc.signer)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, sender, fromTx)
			if tc.err == nil {
				require.Equal(t, tx.SenderAddress, sender.Value())
			}
		})
	}
}

func TestFrameTransactionJSON(t *testing.T) {
	t.Parallel()
	input := []byte(`{"type":"0x6","chainId":"0x1","nonce":"0x7","nonceKeys":["0x1","0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"],"sender":"0x0000000000000000000000000000000000001234","frames":[{"mode":"0x1","flags":"0x3","target":null,"gasLimit":"0x64","stateGasLimit":"0x0","value":"0x0","data":"0x0102"}],"signatures":[{"scheme":"0x0","signer":"0x","msg":"0x","signature":"0xaabb"}],"maxPriorityFeePerGas":"0x2","maxFeePerGas":"0x14","maxFeePerBlobGas":"0x0","blobVersionedHashes":[]}`)
	tx, err := UnmarshalTransactionFromJSON(input)
	require.NoError(t, err)
	require.Equal(t, uint64(7), tx.GetNonce())
	require.Equal(t, []byte{1, 2}, tx.(*FrameTransaction).Frames[0].Data)
	require.Equal(t, []uint256.Int{{1}, *new(uint256.Int).SetAllOne()}, tx.(*FrameTransaction).NonceKeys)
	out, err := json.Marshal(tx)
	require.NoError(t, err)
	decoded, err := UnmarshalTransactionFromJSON(out)
	require.NoError(t, err)
	require.Equal(t, tx.Hash(), decoded.Hash())
}

func TestFrameTransactionBlobWrapper(t *testing.T) {
	t.Parallel()
	tx := &FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []Frame{{}}, BlobVersionedHashes: []common.Hash{{1}}}
	payload, err := rlp.EncodeToBytes([]any{(*frameTransactionRLP)(tx), uint64(1), Blobs{{}}, BlobKzgs{{}}, make(KZGProofs, 128)})
	require.NoError(t, err)
	wrapped := append([]byte{FrameTxType}, payload...)
	decoded, err := DecodeWrappedTransaction(wrapped)
	require.NoError(t, err)
	require.Equal(t, tx.Hash(), decoded.Hash())
	require.IsType(t, tx, decoded.Unwrap())
	var canonical bytes.Buffer
	require.NoError(t, decoded.MarshalBinary(&canonical))
	plain, err := DecodeTransaction(canonical.Bytes())
	require.NoError(t, err)
	require.Equal(t, tx.Hash(), plain.Hash())
	tx.BlobVersionedHashes = nil
	canonical.Reset()
	require.NoError(t, tx.MarshalBinary(&canonical))
	plain, err = DecodeWrappedTransaction(canonical.Bytes())
	require.NoError(t, err)
	require.IsType(t, tx, plain)
}

func TestFrameTransactionUnwrapSidecar(t *testing.T) {
	tx := &FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []Frame{{}}, BlobVersionedHashes: []common.Hash{{1}}}
	wrapper := &FrameTxWrapper{FrameTransaction: tx, WrapperVersion: 1, Blobs: Blobs{{}}, Commitments: BlobKzgs{{}}, Proofs: make(KZGProofs, 128)}
	var wrapped, canonical bytes.Buffer
	require.NoError(t, wrapper.MarshalBinaryWrapped(&wrapped))
	require.NoError(t, tx.MarshalBinary(&canonical))
	plain, err := UnwrapTxPlayloadRlp(wrapped.Bytes())
	require.NoError(t, err)
	require.Len(t, plain, canonical.Len())
	require.Equal(t, canonical.Bytes(), plain)
	plain, err = UnwrapTxPlayloadRlp(canonical.Bytes())
	require.NoError(t, err)
	require.Equal(t, canonical.Bytes(), plain)
}

func TestFrameTransactionCopySidecar(t *testing.T) {
	tx := &FrameTxWrapper{FrameTransaction: &FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []Frame{{}}, BlobVersionedHashes: []common.Hash{{1}}}, WrapperVersion: 1, Blobs: Blobs{{}}, Commitments: BlobKzgs{{}}, Proofs: make(KZGProofs, 128)}
	copied := CopyTxs(Transactions{tx})
	require.IsType(t, tx, copied[0])
	wrapper := copied[0].(*FrameTxWrapper)
	require.Equal(t, byte(1), wrapper.WrapperVersion)
	wrapper.Blobs[0][0] = 1
	require.Zero(t, tx.Blobs[0][0])
}
