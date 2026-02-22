package order

import "testing"

func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got none")
		}
	}()

	fn()
}

func TestAssertListAsc_LastPairChecked_NoPanicOnSorted(t *testing.T) {
	asc := Asc
	keys := [][]byte{
		{1},
		{2},
		{3},
	}

	asc.assertList(keys)
}

func TestAssertListAsc_LastPairChecked_PanicsOnUnsortedLastPair(t *testing.T) {
	asc := Asc
	keys := [][]byte{
		{1},
		{3},
		{2},
	}

	expectPanic(t, func() {
		asc.assertList(keys)
	})
}

func TestAssertListAsc_TwoElements_PanicsWhenOutOfOrder(t *testing.T) {
	asc := Asc
	keys := [][]byte{
		{2},
		{1},
	}

	expectPanic(t, func() {
		asc.assertList(keys)
	})
}

func TestAssertListDesc_LastPairChecked_NoPanicOnSorted(t *testing.T) {
	desc := Desc
	keys := [][]byte{
		{3},
		{2},
		{1},
	}

	desc.assertList(keys)
}

func TestAssertListDesc_LastPairChecked_PanicsOnUnsortedLastPair(t *testing.T) {
	desc := Desc
	keys := [][]byte{
		{3},
		{1},
		{2},
	}

	expectPanic(t, func() {
		desc.assertList(keys)
	})
}

func TestAssertListDesc_TwoElements_PanicsWhenOutOfOrder(t *testing.T) {
	desc := Desc
	keys := [][]byte{
		{1},
		{2},
	}

	expectPanic(t, func() {
		desc.assertList(keys)
	})
}
