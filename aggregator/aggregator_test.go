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
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
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
	db := memdb.New()
	defer db.Close()
	a, err := NewAggregator(tmpDir, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	var rwTx kv.RwTx
	if rwTx, err = db.BeginRw(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer rwTx.Rollback()

	w := a.MakeStateWriter(true /* beforeOn */)
	if err = w.Reset(rwTx, 0); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var account1 = accountWithBalance(1)
	if err = w.UpdateAccountData(int160(1), account1, false /* trace */); err != nil {
		t.Fatal(err)
	}
	if err = w.FinishTx(0, false); err != nil {
		t.Fatal(err)
	}
	if err = w.Aggregate(false /* trace */); err != nil {
		t.Fatal(err)
	}
	if err = rwTx.Commit(); err != nil {
		t.Fatal(err)
	}
	var tx kv.Tx
	if tx, err = db.BeginRo(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	r := a.MakeStateReader(tx, 2)
	var acc []byte
	if acc, err = r.ReadAccountData(int160(1), false /* trace */); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(acc, account1) {
		t.Errorf("read account %x, expected account %x", acc, account1)
	}
	a.Close()
}

func TestLoopAggregator(t *testing.T) {
	tmpDir := t.TempDir()
	db := memdb.New()
	defer db.Close()
	a, err := NewAggregator(tmpDir, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	var account1 = accountWithBalance(1)
	var rwTx kv.RwTx
	defer func() {
		rwTx.Rollback()
	}()
	var tx kv.Tx
	defer func() {
		tx.Rollback()
	}()
	w := a.MakeStateWriter(true /* beforeOn */)
	defer w.Close()
	ctx := context.Background()
	for blockNum := uint64(0); blockNum < 1000; blockNum++ {
		accountKey := int160(blockNum/10 + 1)
		//fmt.Printf("blockNum = %d\n", blockNum)
		if rwTx, err = db.BeginRw(ctx); err != nil {
			t.Fatal(err)
		}
		if err = w.Reset(rwTx, blockNum); err != nil {
			t.Fatal(err)
		}
		if err = w.UpdateAccountData(accountKey, account1, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.FinishTx(blockNum, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.Aggregate(false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = rwTx.Commit(); err != nil {
			t.Fatal(err)
		}
		if tx, err = db.BeginRo(ctx); err != nil {
			t.Fatal(err)
		}
		r := a.MakeStateReader(tx, blockNum+1)
		var acc []byte
		if acc, err = r.ReadAccountData(accountKey, false /* trace */); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
		if !bytes.Equal(acc, account1) {
			t.Errorf("read account %x, expected account %x for block %d", acc, account1, blockNum)
		}
		account1 = accountWithBalance(blockNum + 2)
	}
	if tx, err = db.BeginRo(ctx); err != nil {
		t.Fatal(err)
	}
	blockNum := uint64(1000)
	r := a.MakeStateReader(tx, blockNum)
	for i := uint64(0); i < blockNum/10+1; i++ {
		accountKey := int160(i)
		var expected []byte
		if i > 0 {
			expected = accountWithBalance(i * 10)
		}
		var acc []byte
		if acc, err = r.ReadAccountData(accountKey, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(acc, expected) {
			t.Errorf("read account %x, expected account %x for block %d", acc, expected, i)
		}
	}
	tx.Rollback()
}

func TestRecreateAccountWithStorage(t *testing.T) {
	tmpDir := t.TempDir()
	db := memdb.New()
	defer db.Close()
	a, err := NewAggregator(tmpDir, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	accountKey := int160(1)
	var account1 = accountWithBalance(1)
	var account2 = accountWithBalance(2)
	var rwTx kv.RwTx
	defer func() {
		rwTx.Rollback()
	}()
	var tx kv.Tx
	defer func() {
		tx.Rollback()
	}()
	w := a.MakeStateWriter(true /* beforeOn */)
	defer w.Close()
	ctx := context.Background()
	for blockNum := uint64(0); blockNum < 100; blockNum++ {
		if rwTx, err = db.BeginRw(ctx); err != nil {
			t.Fatal(err)
		}
		if err = w.Reset(rwTx, blockNum); err != nil {
			t.Fatal(err)
		}
		switch blockNum {
		case 1:
			if err = w.UpdateAccountData(accountKey, account1, false /* trace */); err != nil {
				t.Fatal(err)
			}
			for s := uint64(0); s < 100; s++ {
				if err = w.WriteAccountStorage(accountKey, int256(s), uint256.NewInt(s+1), false /* trace */); err != nil {
					t.Fatal(err)
				}
			}
		case 22:
			if err = w.DeleteAccount(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
		case 45:
			if err = w.UpdateAccountData(accountKey, account2, false /* trace */); err != nil {
				t.Fatal(err)
			}
			for s := uint64(50); s < 150; s++ {
				if err = w.WriteAccountStorage(accountKey, int256(s), uint256.NewInt(2*s+1), false /* trace */); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err = w.FinishTx(blockNum, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.Aggregate(false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = rwTx.Commit(); err != nil {
			t.Fatal(err)
		}
		if tx, err = db.BeginRo(ctx); err != nil {
			t.Fatal(err)
		}
		r := a.MakeStateReader(tx, blockNum+1)
		switch blockNum {
		case 1:
			var acc []byte
			if acc, err = r.ReadAccountData(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(account1, acc) {
				t.Errorf("wrong account after block %d, expected %x, got %x", blockNum, account1, acc)
			}
			for s := uint64(0); s < 100; s++ {
				var v *uint256.Int
				if v, err = r.ReadAccountStorage(accountKey, int256(s), false /* trace */); err != nil {
					t.Fatal(err)
				}
				if !uint256.NewInt(s + 1).Eq(v) {
					t.Errorf("wrong storage value after block %d, expected %d, got %s", blockNum, s+1, v)
				}
			}
		case 22, 44:
			var acc []byte
			if acc, err = r.ReadAccountData(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if len(acc) > 0 {
				t.Errorf("wrong account after block %d, expected nil, got %x", blockNum, acc)
			}
			for s := uint64(0); s < 100; s++ {
				var v *uint256.Int
				if v, err = r.ReadAccountStorage(accountKey, int256(s), false /* trace */); err != nil {
					t.Fatal(err)
				}
				if v != nil {
					t.Errorf("wrong storage value after block %d, expected nil, got %s", blockNum, v)
				}
			}
		case 66:
			var acc []byte
			if acc, err = r.ReadAccountData(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(account2, acc) {
				t.Errorf("wrong account after block %d, expected %x, got %x", blockNum, account1, acc)
			}
			for s := uint64(0); s < 150; s++ {
				var v *uint256.Int
				if v, err = r.ReadAccountStorage(accountKey, int256(s), false /* trace */); err != nil {
					t.Fatal(err)
				}
				if s < 50 {
					if v != nil {
						t.Errorf("wrong storage value after block %d, expected nil, got %s", blockNum, v)
					}
				} else if v == nil || !uint256.NewInt(2*s+1).Eq(v) {
					t.Errorf("wrong storage value after block %d, expected %d, got %s", blockNum, 2*s+1, v)
				}
			}
		}
		tx.Rollback()
	}
}

func TestChangeCode(t *testing.T) {
	tmpDir := t.TempDir()
	db := memdb.New()
	defer db.Close()
	a, err := NewAggregator(tmpDir, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	accountKey := int160(1)
	var account1 = accountWithBalance(1)
	var code1 = []byte("This is the code number 1")
	//var code2 = []byte("This is the code number 2")
	var rwTx kv.RwTx
	defer func() {
		rwTx.Rollback()
	}()
	var tx kv.Tx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	w := a.MakeStateWriter(true /* beforeOn */)
	defer w.Close()
	for blockNum := uint64(0); blockNum < 100; blockNum++ {
		if rwTx, err = db.BeginRw(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err = w.Reset(rwTx, blockNum); err != nil {
			t.Fatal(err)
		}
		switch blockNum {
		case 1:
			if err = w.UpdateAccountData(accountKey, account1, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if err = w.UpdateAccountCode(accountKey, code1, false /* trace */); err != nil {
				t.Fatal(err)
			}
		case 25:
			if err = w.DeleteAccount(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
		}
		if err = w.FinishTx(blockNum, false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = w.Aggregate(false /* trace */); err != nil {
			t.Fatal(err)
		}
		if err = rwTx.Commit(); err != nil {
			t.Fatal(err)
		}
		if tx, err = db.BeginRo(context.Background()); err != nil {
			t.Fatal(err)
		}
		r := a.MakeStateReader(tx, blockNum+1)
		switch blockNum {
		case 22:
			var acc []byte
			if acc, err = r.ReadAccountData(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(account1, acc) {
				t.Errorf("wrong account after block %d, expected %x, got %x", blockNum, account1, acc)
			}
			var code []byte
			if code, err = r.ReadAccountCode(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(code1, code) {
				t.Errorf("wrong code after block %d, expected %x, got %x", blockNum, code1, code)
			}
		case 47:
			var code []byte
			if code, err = r.ReadAccountCode(accountKey, false /* trace */); err != nil {
				t.Fatal(err)
			}
			if code != nil {
				t.Errorf("wrong code after block %d, expected nil, got %x", blockNum, code)
			}
		}
		tx.Rollback()
	}
}
