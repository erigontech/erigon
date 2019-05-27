package llrb

import (
	"reflect"
	"testing"
)

func TestAscendGreaterOrEqual(t *testing.T) {
	tree := New()
	tree.InsertNoReplace(Int(4))
	tree.InsertNoReplace(Int(6))
	tree.InsertNoReplace(Int(1))
	tree.InsertNoReplace(Int(3))
	var ary []Item
	tree.AscendGreaterOrEqual(Int(-1), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected := []Item{Int(1), Int(3), Int(4), Int(6)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
	ary = nil
	tree.AscendGreaterOrEqual(Int(3), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected = []Item{Int(3), Int(4), Int(6)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
	ary = nil
	tree.AscendGreaterOrEqual(Int(2), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected = []Item{Int(3), Int(4), Int(6)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
}

func TestAscendGreaterOrEqual1(t *testing.T) {
	tree := New()
	tree.InsertNoReplace(Int(4))
	tree.InsertNoReplace(Int(6))
	tree.InsertNoReplace(Int(1))
	tree.InsertNoReplace(Int(3))
	var ary []Item
	tree.AscendGreaterOrEqual(Int(-1), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected := []Item{Int(1), Int(3), Int(4), Int(6)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
	ary = nil
	tree.AscendGreaterOrEqual1(Int(3), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected = []Item{Int(3), Int(4), Int(6)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
	ary = nil
	tree.AscendGreaterOrEqual1(Int(2), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected = []Item{Int(3), Int(4), Int(6)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
}

func TestDescendLessOrEqual(t *testing.T) {
	tree := New()
	tree.InsertNoReplace(Int(4))
	tree.InsertNoReplace(Int(6))
	tree.InsertNoReplace(Int(1))
	tree.InsertNoReplace(Int(3))
	var ary []Item
	tree.DescendLessOrEqual(Int(10), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected := []Item{Int(6), Int(4), Int(3), Int(1)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
	ary = nil
	tree.DescendLessOrEqual(Int(4), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected = []Item{Int(4), Int(3), Int(1)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
	ary = nil
	tree.DescendLessOrEqual(Int(5), func(i Item) bool {
		ary = append(ary, i)
		return true
	})
	expected = []Item{Int(4), Int(3), Int(1)}
	if !reflect.DeepEqual(ary, expected) {
		t.Errorf("expected %v but got %v", expected, ary)
	}
}

func TestSeekIterator(t *testing.T) {
	tree := New()
	for i := 2; i < 128; i+=2 {
		tree.InsertNoReplace(Int(i))
	}
	it := tree.NewSeekIterator()
	gotFirst := it.SeekTo(tree.Min())
	if gotFirst.(Int) != Int(2) {
		t.Errorf("got %d, want %d", gotFirst, 2)
	}
	got3 := it.SeekTo(Int(3))
	if got3.(Int) != Int(4) {
		t.Errorf("got %d, want %d", got3, 4)
	}
	got33 := it.SeekTo(Int(33))
	if got33.(Int) != Int(34) {
		t.Errorf("got %d, want %d", got33, 34)
	}
	back3 := it.SeekTo(Int(3))
	if back3.(Int) != Int(36) {
		t.Errorf("got %d, want %d", back3, 36)
	}
	again36 := it.SeekTo(Int(36))
	if again36.(Int) != Int(38) {
		t.Errorf("got %d, want %d", again36, 38)
	}
	got129 := it.SeekTo(Int(129))
	if got129 != nil {
		t.Errorf("got %d, want nil", got129)
	}
}
