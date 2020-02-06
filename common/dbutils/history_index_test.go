package dbutils

import (
	"fmt"
	"reflect"
	"testing"
)

func TestHistoryIndex_Search1(t *testing.T) {
	index := NewHistoryIndex()
	index.Append(3).Append(5).Append(8)
	fmt.Println(index.Decode())
	v, _ := index.Search(1)
	if v != 3 {
		t.Fatal("must be 3 but", v)
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
	v, _ = index.Search(8)
	if v != 8 {
		t.Fatal("must be 8")
	}
	_, b := index.Search(9)
	if b {
		t.Fatal("must be not found")
	}
}

func TestHistoryIndex_Search_EmptyIndex(t *testing.T) {
	index := &HistoryIndexBytes{}
	_, b := index.Search(1)
	if b {
		t.FailNow()
	}
}

func TestHistoryIndex_Append(t *testing.T) {
	index := NewHistoryIndex()
	for i := uint64(1); i < 10; i++ {
		index.Append(i)
	}

	res, err := index.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatal("Not equal")
	}

	if index.Len() != 9 {
		t.Fatal()
	}

	index.Remove(9)
	res, err = index.Decode()
	if err != nil {
		t.Fatal(err)
	}

	if index.Len() != 8 {
		t.Fatal()
	}

	if !reflect.DeepEqual(res, []uint64{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatal("Not equal")
	}

	index.Remove(5)
	res, err = index.Decode()
	if err != nil {
		t.Fatal(err)
	}

	if index.Len() != 7 {
		t.Fatal()
	}

	if !reflect.DeepEqual(res, []uint64{1, 2, 3, 4, 6, 7, 8}) {
		t.Fatal("Not equal")
	}

	index.Remove(1)
	res, err = index.Decode()
	if err != nil {
		t.Fatal(err)
	}

	if index.Len() != 6 {
		t.Fatal()
	}

	if !reflect.DeepEqual(res, []uint64{2, 3, 4, 6, 7, 8}) {
		t.Fatal("Not equal")
	}
}
