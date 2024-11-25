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
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/rlp"
)

const RUNS = 100 // for local tests increase this number

type TRand struct {
	rnd *rand.Rand
}

func NewTRand() *TRand {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	return &TRand{rnd: rand.New(src)}
}

func (tr *TRand) RandIntInRange(_min, _max int) int {
	return (tr.rnd.Intn(_max-_min) + _min)
}

func (tr *TRand) RandUint64() *uint64 {
	a := tr.rnd.Uint64()
	return &a
}

func (tr *TRand) RandBig() *big.Int {
	return big.NewInt(int64(tr.rnd.Int()))
}

func (tr *TRand) RandBytes(size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(tr.rnd.Intn(256))
	}
	return arr
}

func (tr *TRand) RandAddress() libcommon.Address {
	return libcommon.Address(tr.RandBytes(20))
}

func (tr *TRand) RandHash() libcommon.Hash {
	return libcommon.Hash(tr.RandBytes(32))
}

func (tr *TRand) RandBloom() Bloom {
	return Bloom(tr.RandBytes(BloomByteLength))
}

func (tr *TRand) RandWithdrawal() *Withdrawal {
	return &Withdrawal{
		Index:     tr.rnd.Uint64(),
		Validator: tr.rnd.Uint64(),
		Address:   tr.RandAddress(),
		Amount:    tr.rnd.Uint64(),
	}
}

func (tr *TRand) RandHeader() *Header {
	wHash := tr.RandHash()
	pHash := tr.RandHash()
	return &Header{
		ParentHash:            tr.RandHash(),                              // libcommon.Hash
		UncleHash:             tr.RandHash(),                              // libcommon.Hash
		Coinbase:              tr.RandAddress(),                           // libcommon.Address
		Root:                  tr.RandHash(),                              // libcommon.Hash
		TxHash:                tr.RandHash(),                              // libcommon.Hash
		ReceiptHash:           tr.RandHash(),                              // libcommon.Hash
		Bloom:                 tr.RandBloom(),                             // Bloom
		Difficulty:            tr.RandBig(),                               // *big.Int
		Number:                tr.RandBig(),                               // *big.Int
		GasLimit:              *tr.RandUint64(),                           // uint64
		GasUsed:               *tr.RandUint64(),                           // uint64
		Time:                  *tr.RandUint64(),                           // uint64
		Extra:                 tr.RandBytes(tr.RandIntInRange(128, 1024)), // []byte
		MixDigest:             tr.RandHash(),                              // libcommon.Hash
		Nonce:                 BlockNonce(tr.RandBytes(8)),                // BlockNonce
		BaseFee:               tr.RandBig(),                               // *big.Int
		WithdrawalsHash:       &wHash,                                     // *libcommon.Hash
		BlobGasUsed:           tr.RandUint64(),                            // *uint64
		ExcessBlobGas:         tr.RandUint64(),                            // *uint64
		ParentBeaconBlockRoot: &pHash,                                     //*libcommon.Hash
	}
}

func (tr *TRand) RandAccessTuple() AccessTuple {
	n := tr.RandIntInRange(1, 5)
	sk := make([]libcommon.Hash, n)
	for i := 0; i < n; i++ {
		sk[i] = tr.RandHash()
	}
	return AccessTuple{
		Address:     tr.RandAddress(),
		StorageKeys: sk,
	}
}

func (tr *TRand) RandAccessList(size int) AccessList {
	al := make([]AccessTuple, size)
	for i := 0; i < size; i++ {
		al[i] = tr.RandAccessTuple()
	}
	return al
}

func (tr *TRand) RandAuthorizations(size int) []Authorization {
	auths := make([]Authorization, size)
	for i := 0; i < size; i++ {
		auths[i] = Authorization{
			ChainID: *tr.RandUint64(),
			Address: tr.RandAddress(),
			Nonce:   *tr.RandUint64(),
			YParity: uint8(*tr.RandUint64()),
			R:       *uint256.NewInt(*tr.RandUint64()),
			S:       *uint256.NewInt(*tr.RandUint64()),
		}
	}
	return auths
}

func (tr *TRand) RandTransaction() Transaction {
	txType := tr.RandIntInRange(0, 5) // LegacyTxType, AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType
	to := tr.RandAddress()
	commonTx := CommonTx{
		Nonce: *tr.RandUint64(),
		Gas:   *tr.RandUint64(),
		To:    &to,
		Value: uint256.NewInt(*tr.RandUint64()), // wei amount
		Data:  tr.RandBytes(tr.RandIntInRange(128, 1024)),
		V:     *uint256.NewInt(*tr.RandUint64()),
		R:     *uint256.NewInt(*tr.RandUint64()),
		S:     *uint256.NewInt(*tr.RandUint64()),
	}
	switch txType {
	case LegacyTxType:
		return &LegacyTx{
			CommonTx: commonTx, //nolint
			GasPrice: uint256.NewInt(*tr.RandUint64()),
		}
	case AccessListTxType:
		return &AccessListTx{
			LegacyTx: LegacyTx{
				CommonTx: commonTx, //nolint
				GasPrice: uint256.NewInt(*tr.RandUint64()),
			},
			ChainID:    uint256.NewInt(*tr.RandUint64()),
			AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
		}
	case DynamicFeeTxType:
		return &DynamicFeeTransaction{
			CommonTx:   commonTx, //nolint
			ChainID:    uint256.NewInt(*tr.RandUint64()),
			Tip:        uint256.NewInt(*tr.RandUint64()),
			FeeCap:     uint256.NewInt(*tr.RandUint64()),
			AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
		}
	case BlobTxType:
		r := *tr.RandUint64()
		return &BlobTx{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   commonTx, //nolint
				ChainID:    uint256.NewInt(*tr.RandUint64()),
				Tip:        uint256.NewInt(*tr.RandUint64()),
				FeeCap:     uint256.NewInt(*tr.RandUint64()),
				AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
			},
			MaxFeePerBlobGas:    uint256.NewInt(r),
			BlobVersionedHashes: tr.RandHashes(tr.RandIntInRange(1, 2)),
		}
	case SetCodeTxType:
		return &SetCodeTransaction{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   commonTx, //nolint
				ChainID:    uint256.NewInt(*tr.RandUint64()),
				Tip:        uint256.NewInt(*tr.RandUint64()),
				FeeCap:     uint256.NewInt(*tr.RandUint64()),
				AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
			},
			Authorizations: tr.RandAuthorizations(tr.RandIntInRange(0, 5)),
		}
	default:
		fmt.Printf("unexpected txType %v", txType)
		panic("unexpected txType")
	}
}

func (tr *TRand) RandHashes(size int) []libcommon.Hash {
	hashes := make([]libcommon.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = tr.RandHash()
	}
	return hashes
}

func (tr *TRand) RandTransactions(size int) []Transaction {
	txns := make([]Transaction, size)
	for i := 0; i < size; i++ {
		txns[i] = tr.RandTransaction()
	}
	return txns
}

func (tr *TRand) RandRawTransactions(size int) [][]byte {
	txns := make([][]byte, size)
	for i := 0; i < size; i++ {
		txns[i] = tr.RandBytes(tr.RandIntInRange(1, 1023))
	}
	return txns
}

func (tr *TRand) RandHeaders(size int) []*Header {
	uncles := make([]*Header, size)
	for i := 0; i < size; i++ {
		uncles[i] = tr.RandHeader()
	}
	return uncles
}

func (tr *TRand) RandWithdrawals(size int) []*Withdrawal {
	withdrawals := make([]*Withdrawal, size)
	for i := 0; i < size; i++ {
		withdrawals[i] = tr.RandWithdrawal()
	}
	return withdrawals
}

func (tr *TRand) RandRawBody() *RawBody {
	return &RawBody{
		Transactions: tr.RandRawTransactions(tr.RandIntInRange(1, 6)),
		Uncles:       tr.RandHeaders(tr.RandIntInRange(1, 6)),
		Withdrawals:  tr.RandWithdrawals(tr.RandIntInRange(1, 6)),
	}
}

func (tr *TRand) RandRawBlock(setNil bool) *RawBlock {
	if setNil {
		return &RawBlock{
			Header: tr.RandHeader(),
			Body: &RawBody{
				Uncles:      nil,
				Withdrawals: nil,
				// Deposits:     nil,
			},
		}
	}

	return &RawBlock{
		Header: tr.RandHeader(),
		Body:   tr.RandRawBody(),
	}
}

func (tr *TRand) RandBody() *Body {
	return &Body{
		Transactions: tr.RandTransactions(tr.RandIntInRange(1, 6)),
		Uncles:       tr.RandHeaders(tr.RandIntInRange(1, 6)),
		Withdrawals:  tr.RandWithdrawals(tr.RandIntInRange(1, 6)),
	}
}

func isEqualBytes(a, b []byte) bool {
	for i := range a {
		if a[i] != b[i] {
			fmt.Printf("%v != %v at %v", a[i], b[i], i)
			return false
		}
	}
	return true
}

func check(t *testing.T, f string, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Errorf("%s mismatch: want %v, got %v", f, want, got)
	}
}

func checkHeaders(t *testing.T, a, b *Header) {
	check(t, "Header.ParentHash", a.ParentHash, b.ParentHash)
	check(t, "Header.UncleHash", a.UncleHash, b.UncleHash)
	check(t, "Header.Coinbase", a.Coinbase, b.Coinbase)
	check(t, "Header.Root", a.Root, b.Root)
	check(t, "Header.TxHash", a.TxHash, b.TxHash)
	check(t, "Header.ReceiptHash", a.ReceiptHash, b.ReceiptHash)
	check(t, "Header.Bloom", a.Bloom, b.Bloom)
	check(t, "Header.Difficulty", a.Difficulty, b.Difficulty)
	check(t, "Header.Number", a.Number, b.Number)
	check(t, "Header.GasLimit", a.GasLimit, b.GasLimit)
	check(t, "Header.GasUsed", a.GasUsed, b.GasUsed)
	check(t, "Header.Time", a.Time, b.Time)
	check(t, "Header.Extra", a.Extra, b.Extra)
	check(t, "Header.MixDigest", a.MixDigest, b.MixDigest)
	check(t, "Header.Nonce", a.Nonce, b.Nonce)
	check(t, "Header.BaseFee", a.BaseFee, b.BaseFee)
	check(t, "Header.WithdrawalsHash", a.WithdrawalsHash, b.WithdrawalsHash)
	check(t, "Header.BlobGasUsed", a.BlobGasUsed, b.BlobGasUsed)
	check(t, "Header.ExcessBlobGas", a.ExcessBlobGas, b.ExcessBlobGas)
	check(t, "Header.ParentBeaconBlockRoot", a.ParentBeaconBlockRoot, b.ParentBeaconBlockRoot)
}

func checkWithdrawals(t *testing.T, a, b *Withdrawal) {
	check(t, "Withdrawal.Index", a.Index, b.Index)
	check(t, "Withdrawal.Validator", a.Validator, b.Validator)
	check(t, "Withdrawal.Address", a.Address, b.Address)
	check(t, "Withdrawal.Amount", a.Amount, b.Amount)
}

func compareTransactions(t *testing.T, a, b Transaction) {
	v1, r1, s1 := a.RawSignatureValues()
	v2, r2, s2 := b.RawSignatureValues()
	check(t, "Tx.Type", a.Type(), b.Type())
	check(t, "Tx.GetChainID", a.GetChainID(), b.GetChainID())
	check(t, "Tx.GetNonce", a.GetNonce(), b.GetNonce())
	check(t, "Tx.GetPrice", a.GetPrice(), b.GetPrice())
	check(t, "Tx.GetTip", a.GetTip(), b.GetTip())
	check(t, "Tx.GetFeeCap", a.GetFeeCap(), b.GetFeeCap())
	check(t, "Tx.GetBlobHashes", a.GetBlobHashes(), b.GetBlobHashes())
	check(t, "Tx.GetGas", a.GetGas(), b.GetGas())
	check(t, "Tx.GetBlobGas", a.GetBlobGas(), b.GetBlobGas())
	check(t, "Tx.GetValue", a.GetValue(), b.GetValue())
	check(t, "Tx.GetTo", a.GetTo(), b.GetTo())
	check(t, "Tx.GetData", a.GetData(), b.GetData())
	check(t, "Tx.GetAccessList", a.GetAccessList(), b.GetAccessList())
	check(t, "Tx.V", v1, v2)
	check(t, "Tx.R", r1, r2)
	check(t, "Tx.S", s1, s2)
}

func compareHeaders(t *testing.T, a, b []*Header) error {
	auLen, buLen := len(a), len(b)
	if auLen != buLen {
		return fmt.Errorf("uncles len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < auLen; i++ {
		checkHeaders(t, a[i], b[i])
	}
	return nil
}

func compareWithdrawals(t *testing.T, a, b []*Withdrawal) error {
	awLen, bwLen := len(a), len(b)
	if awLen != bwLen {
		return fmt.Errorf("withdrawals len mismatch: expected: %v, got: %v", awLen, bwLen)
	}

	for i := 0; i < awLen; i++ {
		checkWithdrawals(t, a[i], b[i])
	}
	return nil
}

func compareRawBodies(t *testing.T, a, b *RawBody) error {

	atLen, btLen := len(a.Transactions), len(b.Transactions)
	if atLen != btLen {
		return fmt.Errorf("transactions len mismatch: expected: %v, got: %v", atLen, btLen)
	}

	for i := 0; i < atLen; i++ {
		if !isEqualBytes(a.Transactions[i], b.Transactions[i]) {
			return fmt.Errorf("byte transactions are not equal")
		}
	}

	compareHeaders(t, a.Uncles, b.Uncles)
	compareWithdrawals(t, a.Withdrawals, b.Withdrawals)
	return nil
}

func compareBodies(t *testing.T, a, b *Body) error {

	atLen, btLen := len(a.Transactions), len(b.Transactions)
	if atLen != btLen {
		return fmt.Errorf("txns len mismatch: expected: %v, got: %v", atLen, btLen)
	}

	for i := 0; i < atLen; i++ {
		compareTransactions(t, a.Transactions[i], b.Transactions[i])
	}

	compareHeaders(t, a.Uncles, b.Uncles)
	compareWithdrawals(t, a.Withdrawals, b.Withdrawals)

	return nil
}

// func TestRawBodyEncodeDecodeRLP(t *testing.T) {
// 	tr := NewTRand()
// 	var buf bytes.Buffer
// 	for i := 0; i < RUNS; i++ {
// 		enc := tr.RandRawBody()
// 		buf.Reset()
// 		if err := enc.EncodeRLP(&buf); err != nil {
// 			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
// 		}

// 		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

// 		dec := &RawBody{}
// 		if err := dec.DecodeRLP(s); err != nil {
// 			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
// 			panic(err)
// 		}

// 		if err := compareRawBodies(t, enc, dec); err != nil {
// 			t.Errorf("error: compareRawBodies: %v", err)
// 		}
// 	}
// }

func TestBodyEncodeDecodeRLP(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for i := 0; i < RUNS; i++ {
		enc := tr.RandBody()
		buf.Reset()
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
		}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec := &Body{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
			panic(err)
		}

		if err := compareBodies(t, enc, dec); err != nil {
			t.Errorf("error: compareBodies: %v", err)
		}
	}
}
