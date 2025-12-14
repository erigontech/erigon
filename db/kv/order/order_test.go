package order

import (
	"testing"

	"github.com/erigontech/erigon/common/dbg"
)

func withAssertsEnabled(t *testing.T, fn func()) {
	t.Helper()
	prev := dbg.AssertEnabled
	dbg.AssertEnabled = true
	defer func() {
		dbg.AssertEnabled = prev
	}()

	fn()
}

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
	withAssertsEnabled(t, func() {
		asc := Asc
		keys := [][]byte{
			{1},
			{2},
			{3},
		}

		asc.AssertList(keys)
	})
}

func TestAssertListAsc_LastPairChecked_PanicsOnUnsortedLastPair(t *testing.T) {
	withAssertsEnabled(t, func() {
		asc := Asc
		keys := [][]byte{
			{1},
			{3},
			{2},
		}

		expectPanic(t, func() {
			asc.AssertList(keys)
		})
	})
}

func TestAssertListAsc_TwoElements_PanicsWhenOutOfOrder(t *testing.T) {
	withAssertsEnabled(t, func() {
		asc := Asc
		keys := [][]byte{
			{2},
			{1},
		}

		expectPanic(t, func() {
			asc.AssertList(keys)
		})
	})
}

func TestAssertListDesc_LastPairChecked_NoPanicOnSorted(t *testing.T) {
	withAssertsEnabled(t, func() {
		desc := Desc
		keys := [][]byte{
			{3},
			{2},
			{1},
		}

		desc.AssertList(keys)
	})
}

func TestAssertListDesc_LastPairChecked_PanicsOnUnsortedLastPair(t *testing.T) {
	withAssertsEnabled(t, func() {
		desc := Desc
		keys := [][]byte{
			{3},
			{1},
			{2},
		}

		expectPanic(t, func() {
			desc.AssertList(keys)
		})
	})
}

func TestAssertListDesc_TwoElements_PanicsWhenOutOfOrder(t *testing.T) {
	withAssertsEnabled(t, func() {
		desc := Desc
		keys := [][]byte{
			{1},
			{2},
		}

		expectPanic(t, func() {
			desc.AssertList(keys)
		})
	})
}
