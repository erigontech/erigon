package ethdb

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"reflect"
	"testing"
)

func TestHistoryIndex_Search(t *testing.T) {
	index := &HistoryIndex{3, 5, 8}
	v, _ := index.Search(1)
	if v != 3 {
		t.Fatal("must be 3")
	}
	v, _ = index.Search(3)
	if v != 3 {
		t.Fatal("must be 3")
	}

	v, _ = index.Search(4)
	if v != 5 {
		t.Fatal("must be 5")
	}

	v, _ = index.Search(5)
	if v != 5 {
		t.Fatal("must be 5")
	}
	v, _ = index.Search(7)
	if v != 8 {
		t.Fatal("must be 8")
	}
	_, b := index.Search(9)
	if b {
		t.Fatal("must be not found")
	}
}

func TestHistoryIndex_Search2(t *testing.T) {
	index := &HistoryIndex{}
	_, b := index.Search(1)
	if b {
		t.FailNow()
	}
}

func TestStorageIndex_Append(t *testing.T) {
	index := NewStorageIndex()
	key1,key2:=common.Hash{1}, common.Hash{2}
	index.Append(key1, 123)
	index.Append(key2, 121)
	index.Append(key2, 321)
	index.Append(key2, 421)

	if !reflect.DeepEqual(index, StorageIndex{
		key1:&HistoryIndex{123},
		key2:&HistoryIndex{121,321,421},
	}) {
		t.Fatal()
	}

	v,found:=index.Search(key2, 121)
	if !found || v!=121 {
		t.Fatal(v,found)
	}

	index.Remove(key1, 123)
	index.Remove(key2, 321)

	if !reflect.DeepEqual(index, StorageIndex{
		key2:&HistoryIndex{121,421},
	}) {
		t.Fatal()
	}

	b,err :=index.Encode()
	if err!=nil {
		t.Fatal(err)
	}

	index2:=NewStorageIndex()
	err=index2.Decode(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(index, index2) {
		t.Fatal()
	}
}