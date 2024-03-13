package types

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/rlp"
)

const RUNS = 100

// random number between min and max
func randIntMinMax(rnd *rand.Rand, min, max int) int {
	return (rnd.Intn(max-min) + min)
}

func randBytes(rnd *rand.Rand, size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(rnd.Intn(256))
	}
	return arr
}

func randHash32(rnd *rand.Rand) libcommon.Hash {
	return libcommon.Hash(randBytes(rnd, 32))
}

func randHash20(rnd *rand.Rand) libcommon.Address {
	return libcommon.Address(randBytes(rnd, 20))
}

func randBloom(rnd *rand.Rand) Bloom {
	return Bloom(randBytes(rnd, BloomByteLength))
}

func randDeposit(rnd *rand.Rand) *Deposit {
	return &Deposit{
		Pubkey:                [pLen]byte(randBytes(rnd, pLen)),
		WithdrawalCredentials: [wLen]byte(randBytes(rnd, wLen)),
		Amount:                rnd.Uint64(),
		Signature:             [sLen]byte(randBytes(rnd, sLen)),
		Index:                 rnd.Uint64(),
	}
}

func randWithdrawal(rnd *rand.Rand) *Withdrawal {
	return &Withdrawal{
		Index:     rnd.Uint64(),
		Validator: rnd.Uint64(),
		Address:   randHash20(rnd),
		Amount:    rnd.Uint64(),
	}
}

func randHeader(rnd *rand.Rand) *Header {
	wHash := randHash32(rnd)
	pHash := randHash32(rnd)
	bgu := rnd.Uint64()
	ebg := rnd.Uint64()
	return &Header{
		ParentHash:            randHash32(rnd),                                         // libcommon.Hash
		UncleHash:             randHash32(rnd),                                         // libcommon.Hash
		Coinbase:              randHash20(rnd),                                         // libcommon.Address
		Root:                  randHash32(rnd),                                         // libcommon.Hash
		TxHash:                randHash32(rnd),                                         // libcommon.Hash
		ReceiptHash:           randHash32(rnd),                                         // libcommon.Hash
		Bloom:                 randBloom(rnd),                                          // Bloom
		Difficulty:            big.NewInt(int64(rnd.Int())),                            // *big.Int
		Number:                big.NewInt(int64(rnd.Int())),                            // *big.Int
		GasLimit:              rnd.Uint64(),                                            // uint64
		GasUsed:               rnd.Uint64(),                                            // uint64
		Time:                  rnd.Uint64(),                                            // uint64
		Extra:                 randBytes(rnd, rnd.Intn(randIntMinMax(rnd, 128, 1024))), // []byte
		MixDigest:             randHash32(rnd),                                         // libcommon.Hash
		Nonce:                 BlockNonce(randBytes(rnd, 8)),                           // BlockNonce
		BaseFee:               big.NewInt(int64(rnd.Int())),                            // *big.Int
		WithdrawalsHash:       &wHash,                                                  // *libcommon.Hash
		BlobGasUsed:           &bgu,                                                    // *uint64
		ExcessBlobGas:         &ebg,                                                    // *uint64
		ParentBeaconBlockRoot: &pHash,                                                  //*libcommon.Hash
	}
}

func randRawTransactions(rnd *rand.Rand, size int) [][]byte {
	txns := make([][]byte, size)
	for i := 0; i < size; i++ {
		txns[i] = randBytes(rnd, randIntMinMax(rnd, 1, 55))
	}
	return txns
}

func randUnles(rnd *rand.Rand, size int) []*Header {
	uncles := make([]*Header, size)
	for i := 0; i < size; i++ {
		uncles[i] = randHeader(rnd)
	}
	return uncles
}

func randWithdrawals(rnd *rand.Rand, size int) []*Withdrawal {
	withdrawals := make([]*Withdrawal, size)
	for i := 0; i < size; i++ {
		withdrawals[i] = randWithdrawal(rnd)
	}
	return withdrawals
}

func randDepoists(rnd *rand.Rand, size int) []*Deposit {
	deposits := make([]*Deposit, size)
	for i := 0; i < size; i++ {
		deposits[i] = randDeposit(rnd)
	}
	return deposits
}

func randRawBody(rnd *rand.Rand) *RawBody {
	return &RawBody{
		Transactions: randRawTransactions(rnd, randIntMinMax(rnd, 1, 6)),
		Uncles:       randUnles(rnd, randIntMinMax(rnd, 1, 6)),
		Withdrawals:  randWithdrawals(rnd, randIntMinMax(rnd, 1, 6)),
		Deposits:     randDepoists(rnd, randIntMinMax(rnd, 1, 6)),
	}
}

func randRawBlock(rnd *rand.Rand, setNil bool) *RawBlock {
	if setNil {
		return &RawBlock{
			Header: randHeader(rnd),
			Body: &RawBody{
				Transactions: nil,
				Uncles:       nil,
				Withdrawals:  nil,
				Deposits:     nil,
			},
		}
	}

	return &RawBlock{
		Header: randHeader(rnd),
		Body:   randRawBody(rnd),
	}
}

func isEqualBytes(a, b []byte) bool {
	for i := range a {
		if a[i] != b[i] {
			fmt.Printf("%v != %v", a[i], b[i])
			return false
		}
	}
	return true
}

func check(t *testing.T, f string, got, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s mismatch: got %v, want %v", f, got, want)
	}
}

func TestDepositEncodeDecodeRLP(t *testing.T) {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	for i := 0; i < RUNS; i++ {
		enc := randDeposit(rnd)
		var buf bytes.Buffer
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Error(err)
		}

		dec := Deposit{}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		if err := dec.DecodeRLP(s); err != nil {
			t.Error(err)
		}

		if !isEqualBytes(enc.Pubkey[:], dec.Pubkey[:]) {
			t.Errorf("deposit decode: not equal Pubkeys: expected: %v, got: %v", enc.Pubkey, dec.Pubkey)
		}

		if !isEqualBytes(enc.WithdrawalCredentials[:], dec.WithdrawalCredentials[:]) {
			t.Errorf("deposit decode: not equal WithdrawalCridentials: expected: %v, got: %v", enc.WithdrawalCredentials, dec.WithdrawalCredentials)
		}

		if enc.Amount != dec.Amount {
			t.Errorf("deposit decode: not equal Amounts: expected: %v, got: %v", enc.Amount, dec.Amount)
		}

		if !isEqualBytes(enc.Signature[:], dec.Signature[:]) {
			t.Errorf("deposit decode: not equal Signature: expected: %v, got: %v", enc.Signature, dec.Signature)
		}

		if enc.Index != dec.Index {
			t.Errorf("deposit decode: not equal Indexes: expected: %v, got: %v", enc.Index, dec.Index)
		}
	}

	for i := 0; i < RUNS; i++ {
		enc := randDeposit(rnd)
		enc.Amount = uint64(i)
		enc.Index = uint64(i)
		var buf bytes.Buffer
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Error(err)
		}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

		dec := Deposit{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Error(err)
		}

		if !isEqualBytes(enc.Pubkey[:], dec.Pubkey[:]) {
			t.Errorf("deposit decode: not equal Pubkeys: expected: %v, got: %v", enc.Pubkey, dec.Pubkey)
		}

		if !isEqualBytes(enc.WithdrawalCredentials[:], dec.WithdrawalCredentials[:]) {
			t.Errorf("deposit decode: not equal WithdrawalCridentials: expected: %v, got: %v", enc.WithdrawalCredentials, dec.WithdrawalCredentials)
		}

		if enc.Amount != dec.Amount {
			t.Errorf("deposit decode: not equal Amounts: expected: %v, got: %v", enc.Amount, dec.Amount)
		}

		if !isEqualBytes(enc.Signature[:], dec.Signature[:]) {
			t.Errorf("deposit decode: not equal Signature: expected: %v, got: %v", enc.Signature, dec.Signature)
		}

		if enc.Index != dec.Index {
			t.Errorf("deposit decode: not equal Indexes: expected: %v, got: %v", enc.Index, dec.Index)
		}
	}
}

func compareHeaders(t *testing.T, a, b *Header) {
	check(t, "Header.ParentHash", a.ParentHash, b.ParentHash)
	check(t, "Withdrawal.UncleHash", a.UncleHash, b.UncleHash)
	check(t, "Withdrawal.Coinbase", a.Coinbase, b.Coinbase)
	check(t, "Withdrawal.Root", a.Root, b.Root)
	check(t, "Withdrawal.TxHash", a.TxHash, b.TxHash)
	check(t, "Withdrawal.ReceiptHash", a.ReceiptHash, b.ReceiptHash)
	check(t, "Withdrawal.Bloom", a.Bloom, b.Bloom)
	check(t, "Withdrawal.Difficulty", a.Difficulty, b.Difficulty)
	check(t, "Withdrawal.Number", a.Number, b.Number)
	check(t, "Withdrawal.GasLimit", a.GasLimit, b.GasLimit)
	check(t, "Withdrawal.GasUsed", a.GasUsed, b.GasUsed)
	check(t, "Withdrawal.Time", a.Time, b.Time)
	check(t, "Withdrawal.Extra", a.Extra, b.Extra)
	check(t, "Withdrawal.MixDigest", a.MixDigest, b.MixDigest)
	check(t, "Withdrawal.Nonce", a.Nonce, b.Nonce)
	check(t, "Withdrawal.BaseFee", a.BaseFee, b.BaseFee)
	check(t, "Withdrawal.WithdrawalsHash", a.WithdrawalsHash, b.WithdrawalsHash)
	check(t, "Withdrawal.BlobGasUsed", a.BlobGasUsed, b.BlobGasUsed)
	check(t, "Withdrawal.ExcessBlobGas", a.ExcessBlobGas, b.ExcessBlobGas)
	check(t, "Withdrawal.ParentBeaconBlockRoot", a.ParentBeaconBlockRoot, b.ParentBeaconBlockRoot)
}

func compareWithdrawals(t *testing.T, a, b *Withdrawal) {
	check(t, "Withdrawal.Index", a.Index, b.Index)
	check(t, "Withdrawal.Validator", a.Validator, b.Validator)
	check(t, "Withdrawal.Address", a.Address, b.Address)
	check(t, "Withdrawal.Amount", a.Amount, b.Amount)
}

func compareDeposits(t *testing.T, a, b *Deposit) {
	check(t, "Deposit.Pubkey", a.Index, b.Index)
	check(t, "Deposit.WithdrawalCredentials", a.WithdrawalCredentials, b.WithdrawalCredentials)
	check(t, "Deposit.Amount", a.Amount, b.Amount)
	check(t, "Deposit.Signature", a.Signature, b.Signature)
	check(t, "Deposit.Index", a.Index, b.Index)
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

	auLen, buLen := len(a.Uncles), len(b.Uncles)
	if auLen != buLen {
		return fmt.Errorf("uncles len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < auLen; i++ {
		compareHeaders(t, a.Uncles[i], b.Uncles[i])
	}

	awLen, bwLen := len(a.Withdrawals), len(b.Withdrawals)
	if awLen != bwLen {
		return fmt.Errorf("withdrawals len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < awLen; i++ {
		compareWithdrawals(t, a.Withdrawals[i], b.Withdrawals[i])
	}

	return nil
}

// func compareBlocks(t *testing.T, a, b *Block) error {

// 	compareHeaders(t, a.header, b.header)

// 	auLen, buLen := len(a.uncles), len(b.uncles)
// 	if auLen != buLen {
// 		return fmt.Errorf("uncles len mismatch: expected: %v, got: %v", auLen, buLen)
// 	}

// 	for i := 0; i < auLen; i++ {
// 		compareHeaders(t, a.uncles[i], b.uncles[i])
// 	}

// 	awLen, bwLen := len(a.withdrawals), len(b.withdrawals)
// 	if awLen != bwLen {
// 		return fmt.Errorf("withdrawals len mismatch: expected: %v, got: %v", auLen, buLen)
// 	}

// 	for i := 0; i < awLen; i++ {
// 		compareWithdrawals(t, a.withdrawals[i], b.withdrawals[i])
// 	}

// 	adLen, bdLen := len(a.deposits), len(b.deposits)
// 	if adLen != bdLen {
// 		return fmt.Errorf("deposits len mismatch: expected: %v, got: %v", adLen, bdLen)
// 	}

// 	for i := 0; i < adLen; i++ {
// 		compareDeposits(t, a.deposits[i], b.deposits[i])
// 	}

// 	return nil
// }

func TestRawBodyEncodeDecodeRLPWithDeposits(t *testing.T) {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	for i := 0; i < RUNS; i++ {
		enc := randRawBody(rnd)
		fmt.Println("DEPOSITS LEN: ", len(enc.Deposits))
		fmt.Println("TRANSACTIONS LEN: ", len(enc.Transactions))
		var buf bytes.Buffer
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
		}
		fmt.Println("buf size: ", buf.Len())
		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

		dec := &RawBody{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
			panic(err)
		}

		if err := compareRawBodies(t, enc, dec); err != nil {
			t.Errorf("error: compareRawBodies: %v", err)
		}
		// rawBlock := randRawBlock(rnd, false)
		// enc, err := rawBlock.AsBlock()
		// if err != nil {
		// 	t.Error(err)
		// }
		// var buf bytes.Buffer
		// if err := enc.EncodeRLP(&buf); err != nil {
		// 	t.Error(err)
		// }

		// s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

		// dec := &Block{}
		// if err := dec.DecodeRLP(s); err != nil {
		// 	t.Errorf("error: Block.DecodeRLP(): %v", err)
		// 	panic(err)
		// }

		// if err := compareBlocks(t, enc, dec); err != nil {
		// 	t.Errorf("error: compareBlocks: %v", err)
		// }
	}
}
