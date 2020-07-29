package dbutils

import (
	"reflect"
	"testing"
)

func TestHistoryIndex_Search1(t *testing.T) {
	index := NewHistoryIndex().Append(3, false).Append(5, false).Append(8, false)
	v, _, _ := index.Search(1)
	if v != 3 {
		t.Fatal("must be 3 but", v)
	}
	v, _, _ = index.Search(3)
	if v != 3 {
		t.Fatal("must be 3")
	}

	v, _, _ = index.Search(4)
	if v != 5 {
		t.Fatal("must be 5")
	}

	v, _, _ = index.Search(5)
	if v != 5 {
		t.Fatal("must be 5")
	}
	v, _, _ = index.Search(7)
	if v != 8 {
		t.Fatal("must be 8")
	}
	v, _, _ = index.Search(8)
	if v != 8 {
		t.Fatal("must be 8")
	}
	_, _, b := index.Search(9)
	if b {
		t.Fatal("must be not found")
	}
}

func TestHistoryIndex_Search_EmptyIndex(t *testing.T) {
	index := NewHistoryIndex()
	_, _, b := index.Search(1)
	if b {
		t.FailNow()
	}
}

func TestHistoryIndex_Append(t *testing.T) {
	index := NewHistoryIndex()
	for i := uint64(1); i < 10; i++ {
		index = index.Append(i, false)
	}

	res, _, err := index.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatal("Not equal")
	}

	if index.Len() != 9 {
		t.Fatal()
	}
}

func TestHistoryIndex_Idempotent(t *testing.T) {
	index := NewHistoryIndex()
	for i := uint64(1); i < 10; i++ {
		index = index.Append(i, false)
	}
	oldLen := len(index)
	for i := uint64(1); i < 10; i++ {
		index = index.Append(i, false)
		if len(index) != oldLen {
			t.Errorf("index is not idempotent, managed to append %d", i)
		}
	}
}
