/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package aggregator

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
)

func int160(i uint64) []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint64(b[12:], i)
	return b
}

func int256(i uint64) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], i)
	return b
}

func accountWithBalance(i uint64) []byte {
	balance := uint256.NewInt(i)
	var l int
	l++
	l++
	if i > 0 {
		l += balance.ByteLen()
	}
	l++
	l++
	value := make([]byte, l)
	pos := 0
	value[pos] = 0
	pos++
	if balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	value[pos] = 0
	pos++
	value[pos] = 0
	return value
}

func TestSimpleAggregator(t *testing.T) {
	tmpDir := t.TempDir()
	a, err := NewAggregator(tmpDir, 16, 4, true, true, 1000)
	if err != nil {
		t.Fatal(err)
	}

	w := a.MakeStateWriter(true /* beforeOn */)
	if err = w.Reset(0); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var account1 = accountWithBalance(1)
	w.UpdateAccountData(int160(1), account1, false /* trace */)
	if err = w.FinishTx(0, false); err != nil {
		t.Fatal(err)
	}
	if err = w.Aggregate(false /* trace */); err != nil {
		t.Fatal(err)
	}
	r := a.MakeStateReader(2)
	acc := r.ReadAccountData(int160(1), false /* trace */)
	if !bytes.Equal(acc, account1) {
		t.Errorf("read account %x, expected account %x", acc, account1)
	}
	a.Close()
}

func TestLoopAggregator(t *testing.T) {
	tmpDir := t.TempDir()
	a, err := NewAggregator(tmpDir, 16, 4, true, true, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	var account1 = accountWithBalance(1)
	w := a.MakeStateWriter(true /* beforeOn */)
	defer w.Close()
	for blockNum := uint64(0); blockNum < 1000; blockNum++ {
		accountKey := int160(blockNum/10 + 1)
		//fmt.Printf("blockNum = %d\n", blockNum)
		if err = w.Reset(blockNum); err != nil {
			t.Fatal(err)
		}
		w.UpdateAccountData(accountKey, account1, false /* trace */)
		if err = w.FinishTx(blockNum, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.Aggregate(false /* trace */); err != nil {
			t.Fatal(err)
		}
		r := a.MakeStateReader(blockNum + 1)
		acc := r.ReadAccountData(accountKey, false /* trace */)
		if !bytes.Equal(acc, account1) {
			t.Errorf("read account %x, expected account %x for block %d", acc, account1, blockNum)
		}
		account1 = accountWithBalance(blockNum + 2)
	}
}

func TestRecreateAccountWithStorage(t *testing.T) {
	tmpDir := t.TempDir()
	a, err := NewAggregator(tmpDir, 16, 4, true, true, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	accountKey := int160(1)
	var account1 = accountWithBalance(1)
	var account2 = accountWithBalance(2)
	w := a.MakeStateWriter(true /* beforeOn */)
	defer w.Close()
	for blockNum := uint64(0); blockNum < 100; blockNum++ {
		if err = w.Reset(blockNum); err != nil {
			t.Fatal(err)
		}
		switch blockNum {
		case 1:
			w.UpdateAccountData(accountKey, account1, false /* trace */)
			for s := uint64(0); s < 100; s++ {
				w.WriteAccountStorage(accountKey, int256(s), uint256.NewInt(s+1), false /* trace */)
			}
		case 22:
			w.DeleteAccount(accountKey, false /* trace */)
		case 45:
			w.UpdateAccountData(accountKey, account2, false /* trace */)
			for s := uint64(50); s < 150; s++ {
				w.WriteAccountStorage(accountKey, int256(s), uint256.NewInt(2*s+1), false /* trace */)
			}
		}
		if err = w.FinishTx(blockNum, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.Aggregate(false /* trace */); err != nil {
			t.Fatal(err)
		}
		r := a.MakeStateReader(blockNum + 1)
		switch blockNum {
		case 1:
			acc := r.ReadAccountData(accountKey, false /* trace */)
			if !bytes.Equal(account1, acc) {
				t.Errorf("wrong account after block %d, expected %x, got %x", blockNum, account1, acc)
			}
			for s := uint64(0); s < 100; s++ {
				v := r.ReadAccountStorage(accountKey, int256(s), false /* trace */)
				if !uint256.NewInt(s + 1).Eq(v) {
					t.Errorf("wrong storage value after block %d, expected %d, got %s", blockNum, s+1, v)
				}
			}
		case 22, 44:
			acc := r.ReadAccountData(accountKey, false /* trace */)
			if len(acc) > 0 {
				t.Errorf("wrong account after block %d, expected nil, got %x", blockNum, acc)
			}
			for s := uint64(0); s < 100; s++ {
				v := r.ReadAccountStorage(accountKey, int256(s), false /* trace */)
				if v != nil {
					t.Errorf("wrong storage value after block %d, expected nil, got %s", blockNum, v)
				}
			}
		case 66:
			acc := r.ReadAccountData(accountKey, false /* trace */)
			if !bytes.Equal(account2, acc) {
				t.Errorf("wrong account after block %d, expected %x, got %x", blockNum, account1, acc)
			}
			for s := uint64(0); s < 150; s++ {
				v := r.ReadAccountStorage(accountKey, int256(s), false /* trace */)
				if s < 50 {
					if v != nil {
						t.Errorf("wrong storage value after block %d, expected nil, got %s", blockNum, v)
					}
				} else if v == nil || !uint256.NewInt(2*s+1).Eq(v) {
					t.Errorf("wrong storage value after block %d, expected %d, got %s", blockNum, 2*s+1, v)
				}
			}
		}
	}
}

func TestChangeCode(t *testing.T) {
	tmpDir := t.TempDir()
	a, err := NewAggregator(tmpDir, 16, 4, true, true, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	accountKey := int160(1)
	var account1 = accountWithBalance(1)
	var code1 = []byte("This is the code number 1")
	w := a.MakeStateWriter(true /* beforeOn */)
	defer w.Close()
	for blockNum := uint64(0); blockNum < 100; blockNum++ {
		if err = w.Reset(blockNum); err != nil {
			t.Fatal(err)
		}
		switch blockNum {
		case 1:
			w.UpdateAccountData(accountKey, account1, false /* trace */)
			w.UpdateAccountCode(accountKey, code1, false /* trace */)
		case 25:
			w.DeleteAccount(accountKey, false /* trace */)
		}
		if err = w.FinishTx(blockNum, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.Aggregate(false /* trace */); err != nil {
			t.Fatal(err)
		}
		r := a.MakeStateReader(blockNum + 1)
		switch blockNum {
		case 22:
			acc := r.ReadAccountData(accountKey, false /* trace */)
			if !bytes.Equal(account1, acc) {
				t.Errorf("wrong account after block %d, expected %x, got %x", blockNum, account1, acc)
			}
			code := r.ReadAccountCode(accountKey, false /* trace */)
			if !bytes.Equal(code1, code) {
				t.Errorf("wrong code after block %d, expected %x, got %x", blockNum, code1, code)
			}
		case 47:
			code := r.ReadAccountCode(accountKey, false /* trace */)
			if code != nil {
				t.Errorf("wrong code after block %d, expected nil, got %x", blockNum, code)
			}
		}
	}
}
