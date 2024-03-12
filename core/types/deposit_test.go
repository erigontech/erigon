package types

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/rlp"
)

const RUNS = 100

func randBytes(rnd *rand.Rand, size int) []byte {
	arr := make([]byte, 128)

	for i := 0; i < size; i++ {
		arr[i] = byte(rnd.Intn(256))
	}

	return arr
}

func randDeposit(rnd *rand.Rand) *Deposit {
	d := &Deposit{
		Pubkey:                [pLen]byte(randBytes(rnd, pLen)),
		WithdrawalCredentials: [wLen]byte(randBytes(rnd, wLen)),
		Amount:                rnd.Uint64(),
		Signature:             [sLen]byte(randBytes(rnd, sLen)),
		Index:                 rnd.Uint64(),
	}

	return d
}

func isEqualBytes(a, b []byte) bool {

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func TestDepositEncodeDecodeRLP(t *testing.T) {
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)

	var enc *Deposit

	for i := 0; i < RUNS; i++ {
		enc = randDeposit(rnd)
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
		fmt.Println("PASS", i)
	}

	for i := 0; i < RUNS; i++ {
		enc = randDeposit(rnd)
		enc.Amount = uint64(i)
		enc.Index = uint64(i)
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
		fmt.Println("PASS", i)
	}
}
