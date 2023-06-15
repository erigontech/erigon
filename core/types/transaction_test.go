// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
)

// The values in those tests are from the Transaction Tests
// at github.com/ethereum/tests.
var (
	testAddr = libcommon.HexToAddress("b94f5374fce5edbc8e2a8697c15331677e6ebf0b")

	emptyTx = NewTransaction(
		0,
		libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"),
		uint256.NewInt(0), 0, uint256.NewInt(0),
		nil,
	)

	rightvrsTx, _ = NewTransaction(
		3,
		testAddr,
		uint256.NewInt(10),
		2000,
		u256.Num1,
		common.FromHex("5544"),
	).WithSignature(
		*LatestSignerForChainID(nil),
		common.Hex2Bytes("98ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4a8887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a301"),
	)

	emptyEip2718Tx = &AccessListTx{
		ChainID: u256.Num1,
		LegacyTx: LegacyTx{
			CommonTx: CommonTx{
				Nonce: 3,
				To:    &testAddr,
				Value: uint256.NewInt(10),
				Gas:   25000,
				Data:  common.FromHex("5544"),
			},
			GasPrice: uint256.NewInt(1),
		},
	}

	signedEip2718Tx, _ = emptyEip2718Tx.WithSignature(
		*LatestSignerForChainID(big.NewInt(1)),
		common.Hex2Bytes("c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b266032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d3752101"),
	)

	dynFeeTx = &DynamicFeeTransaction{
		CommonTx: CommonTx{
			Nonce: 3,
			To:    &testAddr,
			Value: uint256.NewInt(10),
			Gas:   25000,
			Data:  common.FromHex("5544"),
		},
		ChainID: u256.Num1,
		Tip:     uint256.NewInt(1),
		FeeCap:  uint256.NewInt(1),
	}

	signedDynFeeTx, _ = dynFeeTx.WithSignature(
		*LatestSignerForChainID(big.NewInt(1)),
		common.Hex2Bytes("c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b266032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d3752101"),
	)
)

func TestDecodeEmptyInput(t *testing.T) {
	input := []byte{}
	_, err := DecodeTransaction(input)
	if !errors.Is(err, io.EOF) {
		t.Fatal("wrong error:", err)
	}
}

func TestDecodeEmptyTypedTx(t *testing.T) {
	input := []byte{0x80}
	_, err := DecodeTransaction(input)
	if !errors.Is(err, rlp.EOL) {
		t.Fatal("wrong error:", err)
	}
}

func TestTransactionSigHash(t *testing.T) {
	if emptyTx.SigningHash(nil) != libcommon.HexToHash("c775b99e7ad12f50d819fcd602390467e28141316969f4b57f0626f74fe3b386") {
		t.Errorf("empty transaction hash mismatch, got %x", emptyTx.SigningHash(nil))
	}
	if rightvrsTx.SigningHash(nil) != libcommon.HexToHash("fe7a79529ed5f7c3375d06b26b186a8644e0e16c373d7a12be41c62d6042b77a") {
		t.Errorf("RightVRS transaction hash mismatch, got %x", rightvrsTx.SigningHash(nil))
	}
}

func TestTransactionEncode(t *testing.T) {
	txb, err := rlp.EncodeToBytes(rightvrsTx)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	should := common.FromHex("f86103018207d094b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a8255441ca098ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4aa08887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a3")
	if !bytes.Equal(txb, should) {
		t.Errorf("encoded RLP mismatch, got %x", txb)
	}
	assert.False(t, TypedTransactionMarshalledAsRlpString(txb))
}

func TestEIP2718TransactionSigHash(t *testing.T) {
	if emptyEip2718Tx.SigningHash(big.NewInt(1)) != libcommon.HexToHash("49b486f0ec0a60dfbbca2d30cb07c9e8ffb2a2ff41f29a1ab6737475f6ff69f3") {
		t.Errorf("empty EIP-2718 transaction hash mismatch, got %x", emptyEip2718Tx.SigningHash(big.NewInt(1)))
	}
	if signedEip2718Tx.SigningHash(big.NewInt(1)) != libcommon.HexToHash("49b486f0ec0a60dfbbca2d30cb07c9e8ffb2a2ff41f29a1ab6737475f6ff69f3") {
		t.Errorf("signed EIP-2718 transaction hash mismatch, got %x", signedEip2718Tx.SigningHash(big.NewInt(1)))
	}
}

// This test checks signature operations on access list transactions.
func TestEIP2930Signer(t *testing.T) {

	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		keyAddr = crypto.PubkeyToAddress(key.PublicKey)
		signer1 = LatestSignerForChainID(big.NewInt(1))
		signer2 = LatestSignerForChainID(big.NewInt(2))
		tx0     = &AccessListTx{LegacyTx: LegacyTx{CommonTx: CommonTx{Nonce: 1}}}
		tx1     = &AccessListTx{ChainID: u256.Num1, LegacyTx: LegacyTx{CommonTx: CommonTx{Nonce: 1}}}
		tx2, _  = SignNewTx(key, *signer2, &AccessListTx{ChainID: u256.Num2, LegacyTx: LegacyTx{CommonTx: CommonTx{Nonce: 1}}})
	)

	tests := []struct {
		tx             Transaction
		chainID        *big.Int
		signer         *Signer
		wantSignerHash libcommon.Hash
		wantSenderErr  error
		wantSignErr    error
		wantHash       libcommon.Hash // after signing
	}{
		{
			tx:             tx0,
			signer:         signer1,
			chainID:        big.NewInt(1),
			wantSignerHash: libcommon.HexToHash("846ad7672f2a3a40c1f959cd4a8ad21786d620077084d84c8d7c077714caa139"),
			wantSenderErr:  ErrInvalidChainId,
			wantHash:       libcommon.HexToHash("1ccd12d8bbdb96ea391af49a35ab641e219b2dd638dea375f2bc94dd290f2549"),
		},
		{
			tx:             tx1,
			signer:         signer1,
			chainID:        big.NewInt(1),
			wantSenderErr:  ErrInvalidSig,
			wantSignerHash: libcommon.HexToHash("846ad7672f2a3a40c1f959cd4a8ad21786d620077084d84c8d7c077714caa139"),
			wantHash:       libcommon.HexToHash("1ccd12d8bbdb96ea391af49a35ab641e219b2dd638dea375f2bc94dd290f2549"),
		},
		{
			// This checks what happens when trying to sign an unsigned tx for the wrong chain.
			tx:             tx1,
			signer:         signer2,
			chainID:        big.NewInt(2),
			wantSenderErr:  ErrInvalidChainId,
			wantSignerHash: libcommon.HexToHash("367967247499343401261d718ed5aa4c9486583e4d89251afce47f4a33c33362"),
			wantSignErr:    ErrInvalidChainId,
		},
		{
			// This checks what happens when trying to re-sign a signed tx for the wrong chain.
			tx:             tx2,
			signer:         signer1,
			chainID:        big.NewInt(1),
			wantSenderErr:  ErrInvalidChainId,
			wantSignerHash: libcommon.HexToHash("846ad7672f2a3a40c1f959cd4a8ad21786d620077084d84c8d7c077714caa139"),
			wantSignErr:    ErrInvalidChainId,
		},
	}

	for i, test := range tests {
		sigHash := test.tx.SigningHash(test.chainID)
		if sigHash != test.wantSignerHash {
			t.Errorf("test %d: wrong sig hash: got %x, want %x", i, sigHash, test.wantSignerHash)
		}
		sender, err := test.tx.Sender(*test.signer)
		if !errors.Is(err, test.wantSenderErr) {
			t.Errorf("test %d: wrong Sender error %q", i, err)
		}
		if err == nil && sender != keyAddr {
			t.Errorf("test %d: wrong sender address %x", i, sender)
		}
		signedTx, err := SignTx(test.tx, *test.signer, key)
		if !errors.Is(err, test.wantSignErr) {
			t.Fatalf("test %d: wrong SignTx error %q", i, err)
		}
		if signedTx != nil {
			if signedTx.Hash() != test.wantHash {
				t.Errorf("test %d: wrong tx hash after signing: got %x, want %x", i, signedTx.Hash(), test.wantHash)
			}
		}
	}
}

func TestEIP2718TransactionEncode(t *testing.T) {
	// RLP representation
	{
		have, err := rlp.EncodeToBytes(signedEip2718Tx)
		if err != nil {
			t.Fatalf("encode error: %v", err)
		}
		want := common.FromHex("b86601f8630103018261a894b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a825544c001a0c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b2660a032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d37521")
		if !bytes.Equal(have, want) {
			t.Errorf("encoded RLP mismatch, got %x", have)
		}
		assert.True(t, TypedTransactionMarshalledAsRlpString(have))
	}
	// Binary representation
	{
		var buf bytes.Buffer
		if err := signedEip2718Tx.MarshalBinary(&buf); err != nil {
			t.Fatalf("encode error: %v", err)
		}
		have := buf.Bytes()
		want := common.FromHex("01f8630103018261a894b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a825544c001a0c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b2660a032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d37521")
		if !bytes.Equal(have, want) {
			t.Errorf("encoded RLP mismatch, got %x", have)
		}
		assert.False(t, TypedTransactionMarshalledAsRlpString(have))
	}
}
func TestEIP1559TransactionEncode(t *testing.T) {
	{
		var buf bytes.Buffer
		if err := signedDynFeeTx.MarshalBinary(&buf); err != nil {
			t.Fatalf("encode error: %v", err)
		}
		have := buf.Bytes()
		want := common.FromHex("02f864010301018261a894b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a825544c001a0c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b2660a032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d37521")
		if !bytes.Equal(have, want) {
			t.Errorf("encoded RLP mismatch, got %x", have)
		}
		_, err := DecodeTransaction(buf.Bytes())
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		assert.False(t, TypedTransactionMarshalledAsRlpString(have))
	}
}

func decodeTx(data []byte) (Transaction, error) {
	return DecodeTransaction(data)
}

func defaultTestKey() (*ecdsa.PrivateKey, libcommon.Address) {
	key, _ := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	addr := crypto.PubkeyToAddress(key.PublicKey)
	return key, addr
}

func TestRecipientEmpty(t *testing.T) {
	_, addr := defaultTestKey()
	tx, err := decodeTx(common.Hex2Bytes("f8498080808080011ca09b16de9d5bdee2cf56c28d16275a4da68cd30273e2525f3959f5d62557489921a0372ebd8fb3345f7db7b5a86d42e24d36e983e259b0664ceb8c227ec9af572f3d"))
	if err != nil {
		t.Fatal(err)
	}

	from, err := tx.Sender(*LatestSignerForChainID(nil))
	if err != nil {
		t.Fatal(err)
	}
	if addr != from {
		t.Fatal("derived address doesn't match")
	}
}

func TestRecipientNormal(t *testing.T) {
	_, addr := defaultTestKey()

	tx, err := decodeTx(common.Hex2Bytes("f85d80808094000000000000000000000000000000000000000080011ca0527c0d8f5c63f7b9f41324a7c8a563ee1190bcbf0dac8ab446291bdbf32f5c79a0552c4ef0a09a04395074dab9ed34d3fbfb843c2f2546cc30fe89ec143ca94ca6"))
	if err != nil {
		t.Fatal(err)
	}

	from, err := tx.Sender(*LatestSignerForChainID(nil))
	if err != nil {
		t.Fatal(err)
	}
	if addr != from {
		t.Fatal("derived address doesn't match")
	}
}

// Tests that transactions can be correctly sorted according to their price in
// decreasing order, but at the same time with increasing nonces when issued by
// the same account.
func TestTransactionPriceNonceSort(t *testing.T) {
	// Generate a batch of accounts to start with
	keys := make([]*ecdsa.PrivateKey, 25)
	for i := 0; i < len(keys); i++ {
		keys[i], _ = crypto.GenerateKey()
	}
	signer := LatestSignerForChainID(nil)

	// Generate a batch of transactions with overlapping values, but shifted nonces
	idx := map[libcommon.Address]int{}
	groups := TransactionsGroupedBySender{}
	for start, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		for i := 0; i < 25; i++ {
			tx, _ := SignTx(NewTransaction(uint64(start+i), libcommon.Address{}, uint256.NewInt(100), 100, uint256.NewInt(uint64(start+i)), nil), *signer, key)

			j, ok := idx[addr]
			if ok {
				groups[j] = append(groups[j], tx)
			} else {
				idx[addr] = len(groups)
				groups = append(groups, Transactions{tx})
			}
		}
	}
	// Sort the transactions and cross check the nonce ordering
	txset := NewTransactionsByPriceAndNonce(*signer, groups)

	txs := Transactions{}
	for tx := txset.Peek(); tx != nil; tx = txset.Peek() {
		txs = append(txs, tx)
		txset.Shift()
	}
	if len(txs) != 25*25 {
		t.Errorf("expected %d transactions, found %d", 25*25, len(txs))
	}
	for i, txi := range txs {
		fromi, _ := txi.Sender(*signer)

		// Make sure the nonce order is valid
		for j, txj := range txs[i+1:] {
			fromj, _ := txj.Sender(*signer)
			if fromi == fromj && txi.GetNonce() > txj.GetNonce() {
				t.Errorf("invalid nonce ordering: tx #%d (A=%x N=%v) < tx #%d (A=%x N=%v)", i, fromi[:4], txi.GetNonce(), i+j, fromj[:4], txj.GetNonce())
			}
		}
		// If the next tx has different from account, the price must be lower than the current one
		if i+1 < len(txs) {
			next := txs[i+1]
			fromNext, _ := next.Sender(*signer)
			if fromi != fromNext && txi.GetPrice().Cmp(next.GetPrice()) < 0 {
				t.Errorf("invalid gasprice ordering: tx #%d (A=%x P=%v) < tx #%d (A=%x P=%v)", i, fromi[:4], txi.GetPrice(), i+1, fromNext[:4], next.GetPrice())
			}
		}
	}
}

// Tests that if multiple transactions have the same price, the ones seen earlier
// are prioritized to avoid network spam attacks aiming for a specific ordering.
func TestTransactionTimeSort(t *testing.T) {
	// Generate a batch of accounts to start with
	keys := make([]*ecdsa.PrivateKey, 5)
	for i := 0; i < len(keys); i++ {
		keys[i], _ = crypto.GenerateKey()
	}
	signer := LatestSignerForChainID(nil)

	// Generate a batch of transactions with overlapping prices, but different creation times
	idx := map[libcommon.Address]int{}
	groups := TransactionsGroupedBySender{}
	for start, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)

		tx, _ := SignTx(NewTransaction(0, libcommon.Address{}, uint256.NewInt(100), 100, uint256.NewInt(1), nil), *signer, key)
		tx.(*LegacyTx).time = time.Unix(0, int64(len(keys)-start))
		i, ok := idx[addr]
		if ok {
			groups[i] = append(groups[i], tx)
		} else {
			idx[addr] = len(groups)
			groups = append(groups, Transactions{tx})
		}
	}
	// Sort the transactions and cross check the nonce ordering
	txset := NewTransactionsByPriceAndNonce(*signer, groups)

	txs := Transactions{}
	for tx := txset.Peek(); tx != nil; tx = txset.Peek() {
		txs = append(txs, tx)
		txset.Shift()
	}
	if len(txs) != len(keys) {
		t.Errorf("expected %d transactions, found %d", len(keys), len(txs))
	}
	for i, txi := range txs {
		fromi, _ := txi.Sender(*signer)
		if i+1 < len(txs) {
			next := txs[i+1]
			fromNext, _ := next.Sender(*signer)

			if txi.GetPrice().Cmp(next.GetPrice()) < 0 {
				t.Errorf("invalid gasprice ordering: tx #%d (A=%x P=%v) < tx #%d (A=%x P=%v)", i, fromi[:4], txi.GetPrice(), i+1, fromNext[:4], next.GetPrice())
			}
			// Make sure time order is ascending if the txs have the same gas price
			if txi.GetPrice().Cmp(next.GetPrice()) == 0 && txi.(*LegacyTx).time.After(next.(*LegacyTx).time) {
				t.Errorf("invalid received time ordering: tx #%d (A=%x T=%v) > tx #%d (A=%x T=%v)", i, fromi[:4], txi.(*LegacyTx).time, i+1, fromNext[:4], next.(*LegacyTx).time)
			}
		}
	}
}

// TestTransactionCoding tests serializing/de-serializing to/from rlp and JSON.
func TestTransactionCoding(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("could not generate key: %v", err)
	}
	var (
		signer    = LatestSignerForChainID(libcommon.Big1)
		addr      = libcommon.HexToAddress("0x0000000000000000000000000000000000000001")
		recipient = libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87")
		accesses  = types2.AccessList{{Address: addr, StorageKeys: []libcommon.Hash{{0}}}}
	)
	for i := uint64(0); i < 500; i++ {
		var txdata Transaction
		switch i % 5 {
		case 0:
			// Legacy tx.
			txdata = &LegacyTx{
				CommonTx: CommonTx{
					Nonce: i,
					To:    &recipient,
					Gas:   1,
					Data:  []byte("abcdef"),
				},
				GasPrice: u256.Num2,
			}
		case 1:
			// Legacy tx contract creation.
			txdata = &LegacyTx{
				CommonTx: CommonTx{
					Nonce: i,
					Gas:   1,
					Data:  []byte("abcdef"),
				},
				GasPrice: u256.Num2,
			}
		case 2:
			// Tx with non-zero access list.
			txdata = &AccessListTx{
				ChainID: uint256.NewInt(1),
				LegacyTx: LegacyTx{
					CommonTx: CommonTx{
						Nonce: i,
						To:    &recipient,
						Gas:   123457,
						Data:  []byte("abcdef"),
					},
					GasPrice: uint256.NewInt(10),
				},
				AccessList: accesses,
			}
		case 3:
			// Tx with empty access list.
			txdata = &AccessListTx{
				ChainID: uint256.NewInt(1),
				LegacyTx: LegacyTx{
					CommonTx: CommonTx{
						Nonce: i,
						To:    &recipient,
						Gas:   123457,
						Data:  []byte("abcdef"),
					},
					GasPrice: uint256.NewInt(10),
				},
			}
		case 4:
			// Contract creation with access list.
			txdata = &AccessListTx{
				ChainID: uint256.NewInt(1),
				LegacyTx: LegacyTx{
					CommonTx: CommonTx{
						Nonce: i,
						Gas:   123457,
					},
					GasPrice: uint256.NewInt(10),
				},
				AccessList: accesses,
			}
		}
		tx, err := SignNewTx(key, *signer, txdata)
		if err != nil {
			t.Fatalf("could not sign transaction: %v", err)
		}
		// RLP
		parsedTx, err := encodeDecodeBinary(tx)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEqual(parsedTx, tx); err != nil {
			t.Fatal(err)
		}

		// JSON
		parsedTx, err = encodeDecodeJSON(tx)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEqual(parsedTx, tx); err != nil {
			t.Fatal(err)
		}
	}
}

func encodeDecodeJSON(tx Transaction) (Transaction, error) {
	data, err := json.Marshal(tx)
	if err != nil {
		return nil, fmt.Errorf("json encoding failed: %w", err)
	}
	var parsedTx Transaction
	if parsedTx, err = UnmarshalTransactionFromJSON(data); err != nil {
		return nil, fmt.Errorf("json decoding failed: %w", err)
	}
	return parsedTx, nil
}

func encodeDecodeBinary(tx Transaction) (Transaction, error) {
	var buf bytes.Buffer
	var err error
	if err = tx.MarshalBinary(&buf); err != nil {
		return nil, fmt.Errorf("rlp encoding failed: %w", err)
	}
	var parsedTx Transaction
	if parsedTx, err = UnmarshalTransactionFromBinary(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("rlp decoding failed: %w", err)
	}
	return parsedTx, nil
}

func assertEqual(orig Transaction, cpy Transaction) error {
	// compare nonce, price, gaslimit, recipient, amount, payload, V, R, S
	if want, got := orig.Hash(), cpy.Hash(); want != got {
		return fmt.Errorf("parsed tx differs from original tx, want %v, got %v", want, got)
	}
	if want, got := orig.GetChainID(), cpy.GetChainID(); want.Cmp(got) != 0 {
		return fmt.Errorf("invalid chain id, want %d, got %d", want, got)
	}
	if orig.GetAccessList() != nil {
		if !reflect.DeepEqual(orig.GetAccessList(), cpy.GetAccessList()) {
			return fmt.Errorf("access list wrong")
		}
		if orig.GetChainID().Cmp(cpy.GetChainID()) != 0 {
			return fmt.Errorf("invalid chain id, want %d, got %d", orig.GetChainID(), cpy.GetChainID())
		}
	}
	return nil
}

const N = 50

var dummySignedBlobTxs = [N]*SignedBlobTx{}
var addr [20]byte

func randIntInRange(min, max int) int {
	return (rand.Intn(max-min) + min)
}

func randAddr() *libcommon.Address {
	var a libcommon.Address
	for j := 0; j < 20; j++ {
		a[j] = byte(rand.Intn(255))
	}
	return &a
}

func randHash() libcommon.Hash {
	var h libcommon.Hash
	for i := 0; i < 32; i++ {
		h[i] = byte(rand.Intn(255))
	}
	return h
}

func randHashes(n int) []libcommon.Hash {
	h := make([]libcommon.Hash, n)
	for i := 0; i < n; i++ {
		h[i] = randHash()
	}
	return h
}

func randAccessList() types2.AccessList {
	size := randIntInRange(4, 10)
	var result types2.AccessList
	for i := 0; i < size; i++ {
		var tup types2.AccessTuple

		tup.Address = *randAddr()
		tup.StorageKeys = append(tup.StorageKeys, randHash())
		result = append(result, tup)
	}
	return result
}

func randData() []byte {
	data := make([]byte, 0, (1 << 16))
	for j := 0; j < rand.Intn(1<<16); j++ {
		data = append(data, byte(rand.Intn(255)))
	}
	return data
}

func newRandSignedBlobTx() *SignedBlobTx {
	stx := &SignedBlobTx{
		ChainID:              uint256.NewInt(rand.Uint64()),
		Nonce:                rand.Uint64(),
		MaxPriorityFeePerGas: uint256.NewInt(rand.Uint64()),
		MaxFeePerGas:         uint256.NewInt(rand.Uint64()),
		Gas:                  rand.Uint64(),
		To:                   randAddr(),
		Value:                uint256.NewInt(rand.Uint64()),
		Data:                 randData(),
		AccessList:           randAccessList(),

		MaxFeePerDataGas:    uint256.NewInt(rand.Uint64()),
		BlobVersionedHashes: randHashes(randIntInRange(5, 10)),

		YParity: rand.Intn(2) == 1,
		R:       *uint256.NewInt(rand.Uint64()),
		S:       *uint256.NewInt(rand.Uint64()),
	}
	return stx
}

func printSTX(stx *SignedBlobTx) {
	fmt.Println("--SignedBlobTx")
	fmt.Printf("ChainID: %v\n", stx.ChainID)
	fmt.Printf("Nonce: %v\n", stx.Nonce)
	fmt.Printf("MaxPriorityFeePerGas: %v\n", stx.MaxPriorityFeePerGas)
	fmt.Printf("MaxFeePerGas: %v\n", stx.MaxFeePerGas)
	fmt.Printf("Gas: %v\n", stx.Gas)
	fmt.Printf("To: %v\n", stx.To)
	fmt.Printf("Value: %v\n", stx.Value)
	fmt.Printf("Data: %v\n", stx.Data)
	fmt.Printf("AccessList: %v\n", stx.AccessList)
	fmt.Printf("MaxFeePerDataGas: %v\n", stx.MaxFeePerDataGas)
	fmt.Printf("BlobVersionedHashes: %v\n", stx.BlobVersionedHashes)
	fmt.Printf("YParity: %v\n", stx.YParity)
	fmt.Printf("R: %v\n", stx.R)
	fmt.Printf("S: %v\n", stx.S)
	fmt.Println("-----")
	fmt.Println()
}

func populateSignedBlobTxs() {
	for i := 0; i < N; i++ {
		dummySignedBlobTxs[i] = newRandSignedBlobTx()
	}
}

func TestSignedBlobTxEncodeDecode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	populateSignedBlobTxs()
	for i := 0; i < N; i++ {
		// printSTX(dummySignedBlobTxs[i])

		tx, err := encodeDecodeBinary(dummySignedBlobTxs[i])
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(dummySignedBlobTxs[i], tx)
	}
}
