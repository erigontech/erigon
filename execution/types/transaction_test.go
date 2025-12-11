// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
)

// The values in those tests are from the Transaction Tests
// at github.com/ethereum/tests.
var (
	testAddr = common.HexToAddress("b94f5374fce5edbc8e2a8697c15331677e6ebf0b")

	emptyTx = NewTransaction(
		0,
		common.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"),
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
				Nonce:    3,
				To:       &testAddr,
				Value:    uint256.NewInt(10),
				GasLimit: 25000,
				Data:     common.FromHex("5544"),
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
			Nonce:    3,
			To:       &testAddr,
			Value:    uint256.NewInt(10),
			GasLimit: 25000,
			Data:     common.FromHex("5544"),
		},
		ChainID: u256.Num1,
		TipCap:  uint256.NewInt(1),
		FeeCap:  uint256.NewInt(1),
	}

	signedDynFeeTx, _ = dynFeeTx.WithSignature(
		*LatestSignerForChainID(big.NewInt(1)),
		common.Hex2Bytes("c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b266032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d3752101"),
	)
)

func TestDecodeEmptyInput(t *testing.T) {
	t.Parallel()
	input := []byte{}
	_, err := DecodeTransaction(input)
	if !errors.Is(err, io.EOF) {
		t.Fatal("wrong error:", err)
	}
}

func TestDecodeEmptyTypedTx(t *testing.T) {
	t.Parallel()
	input := []byte{0x80}
	_, err := DecodeTransaction(input)
	if !errors.Is(err, rlp.EOL) {
		t.Fatal("wrong error:", err)
	}
}

func TestTransactionSigHash(t *testing.T) {
	t.Parallel()
	if emptyTx.SigningHash(nil) != common.HexToHash("c775b99e7ad12f50d819fcd602390467e28141316969f4b57f0626f74fe3b386") {
		t.Errorf("empty transaction hash mismatch, got %x", emptyTx.SigningHash(nil))
	}
	if rightvrsTx.SigningHash(nil) != common.HexToHash("fe7a79529ed5f7c3375d06b26b186a8644e0e16c373d7a12be41c62d6042b77a") {
		t.Errorf("RightVRS transaction hash mismatch, got %x", rightvrsTx.SigningHash(nil))
	}
}

func TestTransactionEncode(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	if emptyEip2718Tx.SigningHash(big.NewInt(1)) != common.HexToHash("49b486f0ec0a60dfbbca2d30cb07c9e8ffb2a2ff41f29a1ab6737475f6ff69f3") {
		t.Errorf("empty EIP-2718 transaction hash mismatch, got %x", emptyEip2718Tx.SigningHash(big.NewInt(1)))
	}
	if signedEip2718Tx.SigningHash(big.NewInt(1)) != common.HexToHash("49b486f0ec0a60dfbbca2d30cb07c9e8ffb2a2ff41f29a1ab6737475f6ff69f3") {
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
		wantSignerHash common.Hash
		wantSenderErr  error
		wantSignErr    error
		wantHash       common.Hash // after signing
	}{
		{
			tx:             tx0,
			signer:         signer1,
			chainID:        big.NewInt(1),
			wantSignerHash: common.HexToHash("846ad7672f2a3a40c1f959cd4a8ad21786d620077084d84c8d7c077714caa139"),
			wantSenderErr:  ErrInvalidChainId,
			wantHash:       common.HexToHash("1ccd12d8bbdb96ea391af49a35ab641e219b2dd638dea375f2bc94dd290f2549"),
		},
		{
			tx:             tx1,
			signer:         signer1,
			chainID:        big.NewInt(1),
			wantSenderErr:  ErrInvalidSig,
			wantSignerHash: common.HexToHash("846ad7672f2a3a40c1f959cd4a8ad21786d620077084d84c8d7c077714caa139"),
			wantHash:       common.HexToHash("1ccd12d8bbdb96ea391af49a35ab641e219b2dd638dea375f2bc94dd290f2549"),
		},
		{
			// This checks what happens when trying to sign an unsigned txn for the wrong chain.
			tx:             tx1,
			signer:         signer2,
			chainID:        big.NewInt(2),
			wantSenderErr:  ErrInvalidChainId,
			wantSignerHash: common.HexToHash("367967247499343401261d718ed5aa4c9486583e4d89251afce47f4a33c33362"),
			wantSignErr:    ErrInvalidChainId,
		},
		{
			// This checks what happens when trying to re-sign a signed txn for the wrong chain.
			tx:             tx2,
			signer:         signer1,
			chainID:        big.NewInt(1),
			wantSenderErr:  ErrInvalidChainId,
			wantSignerHash: common.HexToHash("846ad7672f2a3a40c1f959cd4a8ad21786d620077084d84c8d7c077714caa139"),
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
				t.Errorf("test %d: wrong txn hash after signing: got %x, want %x", i, signedTx.Hash(), test.wantHash)
			}
		}
	}
}

func TestEIP2718TransactionEncode(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func defaultTestKey() (*ecdsa.PrivateKey, common.Address) {
	key, _ := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	addr := crypto.PubkeyToAddress(key.PublicKey)
	return key, addr
}

func TestRecipientEmpty(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	// Generate a batch of accounts to start with
	keys := make([]*ecdsa.PrivateKey, 25)
	for i := 0; i < len(keys); i++ {
		keys[i], _ = crypto.GenerateKey()
	}
	signer := LatestSignerForChainID(nil)

	// Generate a batch of transactions with overlapping values, but shifted nonces
	idx := map[common.Address]int{}
	groups := TransactionsGroupedBySender{}
	for start, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		for i := 0; i < 25; i++ {
			tx, _ := SignTx(NewTransaction(uint64(start+i), common.Address{}, uint256.NewInt(100), 100, uint256.NewInt(uint64(start+i)), nil), *signer, key)

			j, ok := idx[addr]
			if ok {
				groups[j] = append(groups[j], tx)
			} else {
				idx[addr] = len(groups)
				groups = append(groups, Transactions{tx})
			}
		}
	}
}

// TestTransactionCoding tests serializing/de-serializing to/from rlp and JSON.
func TestTransactionCoding(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("could not generate key: %v", err)
	}
	var (
		signer    = LatestSignerForChainID(common.Big1)
		addr      = common.HexToAddress("0x0000000000000000000000000000000000000001")
		recipient = common.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87")
		accesses  = AccessList{{Address: addr, StorageKeys: []common.Hash{{0}}}}
	)
	for i := uint64(0); i < 500; i++ {
		var txdata Transaction
		switch i % 5 {
		case 0:
			// Legacy tx.
			txdata = &LegacyTx{
				CommonTx: CommonTx{
					Nonce:    i,
					To:       &recipient,
					GasLimit: 1,
					Data:     []byte("abcdef"),
				},
				GasPrice: u256.Num2,
			}
		case 1:
			// Legacy txn contract creation.
			txdata = &LegacyTx{
				CommonTx: CommonTx{
					Nonce:    i,
					GasLimit: 1,
					Data:     []byte("abcdef"),
				},
				GasPrice: u256.Num2,
			}
		case 2:
			// txn with non-zero access list.
			txdata = &AccessListTx{
				ChainID: uint256.NewInt(1),
				LegacyTx: LegacyTx{
					CommonTx: CommonTx{
						Nonce:    i,
						To:       &recipient,
						GasLimit: 123457,
						Data:     []byte("abcdef"),
					},
					GasPrice: uint256.NewInt(10),
				},
				AccessList: accesses,
			}
		case 3:
			// txn with empty access list.
			txdata = &AccessListTx{
				ChainID: uint256.NewInt(1),
				LegacyTx: LegacyTx{
					CommonTx: CommonTx{
						Nonce:    i,
						To:       &recipient,
						GasLimit: 123457,
						Data:     []byte("abcdef"),
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
						Nonce:    i,
						GasLimit: 123457,
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
	if parsedTx, err = UnmarshalTransactionFromBinary(buf.Bytes(), false /* blobTxnsAreWrappedWithBlobs */); err != nil {
		return nil, fmt.Errorf("rlp decoding failed: %w", err)
	}
	return parsedTx, nil
}

func assertEqual(orig Transaction, cpy Transaction) error {
	// compare nonce, price, gaslimit, recipient, amount, payload, V, R, S
	if want, got := orig.Hash(), cpy.Hash(); want != got {
		return fmt.Errorf("parsed txn differs from original tx, want %v, got %v", want, got)
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

func assertEqualBlobWrapper(orig *BlobTxWrapper, cpy *BlobTxWrapper) error {
	// compare commitments, blobs, proofs
	if want, got := len(orig.Commitments), len(cpy.Commitments); want != got {
		return fmt.Errorf("parsed txn commitments have unequal size: want%v, got %v", want, got)
	}

	if want, got := len(orig.Blobs), len(cpy.Blobs); want != got {
		return fmt.Errorf("parsed txn blobs have unequal size: want%v, got %v", want, got)
	}

	if want, got := len(orig.Proofs), len(cpy.Proofs); want != got {
		return fmt.Errorf("parsed txn proofs have unequal size: want%v, got %v", want, got)
	}

	if want, got := orig.Commitments, cpy.Commitments; !reflect.DeepEqual(want, got) {
		return fmt.Errorf("parsed txn commitments unequal: want%v, got %v", want, got)
	}

	if want, got := orig.Blobs, cpy.Blobs; !reflect.DeepEqual(want, got) {
		return fmt.Errorf("parsed txn blobs unequal: want%v, got %v", want, got)
	}

	if want, got := orig.Proofs, cpy.Proofs; !reflect.DeepEqual(want, got) {
		return fmt.Errorf("parsed txn proofs unequal: want%v, got %v", want, got)
	}

	return nil
}

const N = 50

var dummyBlobTxs = [N]*BlobTx{}
var dummyBlobWrapperTxs = [N]*BlobTxWrapper{}

func randIntInRange(_min, _max int) int {
	return (rand.Intn(_max-_min) + _min)
}

func randAddr() *common.Address {
	var a common.Address
	for j := 0; j < 20; j++ {
		a[j] = byte(rand.Intn(255))
	}
	return &a
}

func randHash() common.Hash {
	var h common.Hash
	for i := 0; i < 32; i++ {
		h[i] = byte(rand.Intn(255))
	}
	return h
}

func randHashes(n int) []common.Hash {
	h := make([]common.Hash, n)
	for i := 0; i < n; i++ {
		h[i] = randHash()
	}
	return h
}

func randAccessList() AccessList {
	size := randIntInRange(4, 10)
	var result AccessList
	for i := 0; i < size; i++ {
		var tup AccessTuple

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

func newRandBlobTx() *BlobTx {
	stx := &BlobTx{DynamicFeeTransaction: DynamicFeeTransaction{
		CommonTx: CommonTx{
			Nonce:    rand.Uint64(),
			GasLimit: rand.Uint64(),
			To:       randAddr(),
			Value:    uint256.NewInt(rand.Uint64()),
			Data:     randData(),
			V:        *uint256.NewInt(0),
			R:        *uint256.NewInt(rand.Uint64()),
			S:        *uint256.NewInt(rand.Uint64()),
		},
		ChainID:    uint256.NewInt(rand.Uint64()),
		TipCap:     uint256.NewInt(rand.Uint64()),
		FeeCap:     uint256.NewInt(rand.Uint64()),
		AccessList: randAccessList(),
	},
		MaxFeePerBlobGas:    uint256.NewInt(rand.Uint64()),
		BlobVersionedHashes: randHashes(randIntInRange(1, 6)),
	}
	return stx
}

func printSTX(stx *BlobTx) {
	fmt.Println("--BlobTx")
	fmt.Printf("ChainID: %v\n", stx.ChainID)
	fmt.Printf("Nonce: %v\n", stx.Nonce)
	fmt.Printf("MaxPriorityFeePerGas: %v\n", stx.TipCap)
	fmt.Printf("MaxFeePerGas: %v\n", stx.FeeCap)
	fmt.Printf("Gas: %v\n", stx.GasLimit)
	fmt.Printf("To: %v\n", stx.To)
	fmt.Printf("Value: %v\n", stx.Value)
	fmt.Printf("Data: %v\n", stx.Data)
	fmt.Printf("AccessList: %v\n", stx.AccessList)
	fmt.Printf("MaxFeePerBlobGas: %v\n", stx.MaxFeePerBlobGas)
	fmt.Printf("BlobVersionedHashes: %v\n", stx.BlobVersionedHashes)
	fmt.Printf("V: %v\n", stx.V)
	fmt.Printf("R: %v\n", stx.R)
	fmt.Printf("S: %v\n", stx.S)
	fmt.Println("-----")
	fmt.Println()
}

func printSTXW(txw *BlobTxWrapper) {
	fmt.Println("--BlobTxWrapper")
	printSTX(&txw.Tx)
	fmt.Printf("Commitments LEN: %v\n", txw.Commitments)
	fmt.Printf("Proofs LEN: %v\n", txw.Proofs)
	fmt.Println("-----")
	fmt.Println()
}

func randByte() byte {
	return byte(rand.Intn(256))
}

func newRandCommitments(size int) BlobKzgs {
	var result BlobKzgs
	for i := 0; i < size; i++ {
		var arr [LEN_48]byte
		for j := 0; j < LEN_48; j++ {
			arr[j] = randByte()
		}
		result = append(result, arr)
	}
	return result
}

func newRandProofs(size int) KZGProofs {
	var result KZGProofs
	for i := 0; i < size; i++ {
		var arr [LEN_48]byte
		for j := 0; j < LEN_48; j++ {
			arr[j] = randByte()
		}
		result = append(result, arr)
	}
	return result
}

func newRandBlobs(size int) Blobs {
	var result Blobs
	for i := 0; i < size; i++ {
		var arr [params.BlobSize]byte
		for j := 0; j < params.BlobSize; j++ {
			arr[j] = randByte()
		}
		result = append(result, arr)
	}
	return result
}

func newRandBlobWrapper() *BlobTxWrapper {
	btxw := newRandBlobTx()
	l := len(btxw.BlobVersionedHashes)
	return &BlobTxWrapper{
		Tx:          *btxw, //nolint
		Commitments: newRandCommitments(l),
		Blobs:       newRandBlobs(l),
		Proofs:      newRandProofs(l),
	}
}

func populateBlobTxs() {
	for i := 0; i < N; i++ {
		dummyBlobTxs[i] = newRandBlobTx()
	}
}

func TestBlobTxEncodeDecode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	populateBlobTxs()
	for i := 0; i < N; i++ {
		// printSTX(dummyBlobTxs[i])

		tx, err := encodeDecodeBinary(dummyBlobTxs[i])
		if errors.Is(err, ErrNilToFieldTx) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := assertEqual(dummyBlobTxs[i], tx); err != nil {
			t.Fatal(err)
		}

		// JSON
		tx, err = encodeDecodeJSON(dummyBlobTxs[i])
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEqual(dummyBlobTxs[i], tx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestShortUnwrap(t *testing.T) {
	blobTxRlp, _ := MakeBlobTxnRlp()
	shortRlp, err := UnwrapTxPlayloadRlp(blobTxRlp)
	if err != nil {
		t.Errorf("short rlp stripping failed: %v", err)
		return
	}
	blobTx, err := DecodeTransaction(shortRlp)

	if err != nil {
		t.Errorf("short rlp decoding failed : %v", err)
	}
	wrappedBlobTx := BlobTxWrapper{}
	blockTxRlp2, _ := MakeBlobTxnRlp()
	err = wrappedBlobTx.DecodeRLP(rlp.NewStream(bytes.NewReader(blockTxRlp2[1:]), 0))
	if err != nil {
		t.Errorf("long rlp decoding failed: %v", err)
	}

	assertEqual(blobTx, &wrappedBlobTx.Tx)
}

func TestV1BlobTxnUnwrap(t *testing.T) {
	blobTxRlp, _ := MakeV1WrappedBlobTxnRlp()
	shortRlp, err := UnwrapTxPlayloadRlp(blobTxRlp)
	if err != nil {
		t.Errorf("short rlp stripping failed: %v", err)
		return
	}
	blobTx, err := DecodeTransaction(shortRlp)

	if err != nil {
		t.Errorf("short rlp decoding failed : %v", err)
	}
	wrappedBlobTx := BlobTxWrapper{}
	blockTxRlp2, _ := MakeV1WrappedBlobTxnRlp()
	err = wrappedBlobTx.DecodeRLP(rlp.NewStream(bytes.NewReader(blockTxRlp2[1:]), 0))
	if err != nil {
		t.Errorf("long rlp decoding failed: %v", err)
	}

	assertEqual(blobTx, &wrappedBlobTx.Tx)
}

func TestTrailingBytes(t *testing.T) {
	// Create a valid transaction
	valid_rlp_transaction := []byte{201, 38, 38, 128, 128, 107, 58, 42, 38, 42}

	// Test valid transaction
	transactions := make([][]byte, 1)
	transactions[0] = valid_rlp_transaction

	for _, txn := range transactions {
		if TypedTransactionMarshalledAsRlpString(txn) {
			panic("TypedTransactionMarshalledAsRlpString() error")
		}
	}

	_, err := DecodeTransactions(transactions)
	if err != nil {
		fmt.Println("Valid transaction errored")
		panic(err) // @audit this will pass
	}

	// Append excess bytes to the blob transaction
	num_excess := 100
	malicious_rlp_transaction := make([]byte, len(valid_rlp_transaction)+num_excess)
	copy(malicious_rlp_transaction, valid_rlp_transaction)

	// Validate transactions are different
	assert.NotEqual(t, malicious_rlp_transaction, valid_rlp_transaction)

	// Test malicious transaction
	transactions[0] = malicious_rlp_transaction

	for _, txn := range transactions {
		if TypedTransactionMarshalledAsRlpString(txn) {
			panic("TypedTransactionMarshalledAsRlpString() error")
		}
	}

	_, err = DecodeTransactions(transactions)
	if err == nil {
		panic("Malicious transaction has not errored!") // @audit this panic is occurs
	}
}
