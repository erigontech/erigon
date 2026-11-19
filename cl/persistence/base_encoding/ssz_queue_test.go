package base_encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func pendingWithdrawalList(values ...*solid.PendingPartialWithdrawal) *solid.ListSSZ[*solid.PendingPartialWithdrawal] {
	list := solid.NewStaticListSSZ[*solid.PendingPartialWithdrawal](1000, 24)
	for _, value := range values {
		list.Append(value)
	}
	return list
}

func assertPendingWithdrawalListEqual(t *testing.T, want, got *solid.ListSSZ[*solid.PendingPartialWithdrawal]) {
	t.Helper()
	if want.Len() != got.Len() {
		t.Fatalf("queue lengths differ: want %d, got %d", want.Len(), got.Len())
	}
	for i := 0; i < want.Len(); i++ {
		if *want.Get(i) != *got.Get(i) {
			t.Fatalf("queue element %d differs: want %v, got %v", i, want.Get(i), got.Get(i))
		}
	}
}

func encodePendingWithdrawalDiff(t *testing.T, previous, updated *solid.ListSSZ[*solid.PendingPartialWithdrawal], equal func(a, b *solid.PendingPartialWithdrawal) bool) []byte {
	t.Helper()
	encoder := NewSSZQueueEncoder(equal)
	encoder.Initialize(previous)
	var diff bytes.Buffer
	if err := encoder.WriteDiff(&diff, updated); err != nil {
		t.Fatal(err)
	}
	return diff.Bytes()
}

func TestSSZQueueDiffKeepsSameListNonTailReplacement(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1, Amount: 10}
	b := &solid.PendingPartialWithdrawal{ValidatorIndex: 2, Amount: 20}
	changed := &solid.PendingPartialWithdrawal{ValidatorIndex: 1, Amount: 30}
	list := pendingWithdrawalList(a, b)
	original := pendingWithdrawalList(a, b)

	encoder := NewSSZQueueEncoder(func(a, b *solid.PendingPartialWithdrawal) bool { return *a == *b })
	encoder.Initialize(list)
	list.Set(0, changed)
	var diff bytes.Buffer
	if err := encoder.WriteDiff(&diff, list); err != nil {
		t.Fatal(err)
	}
	if err := ApplySSZQueueDiff(&diff, original, 0); err != nil {
		t.Fatal(err)
	}

	assertPendingWithdrawalListEqual(t, list, original)
}

func TestSSZQueueDiffDuplicateLastAppend(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1, Amount: 10}
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{a}, []*solid.PendingPartialWithdrawal{a, a})
}

func TestSSZQueueDiffFrontCutAndPostponedReappend(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1}
	b := &solid.PendingPartialWithdrawal{ValidatorIndex: 2}
	c := &solid.PendingPartialWithdrawal{ValidatorIndex: 3}
	tests := []struct {
		name     string
		previous []*solid.PendingPartialWithdrawal
		updated  []*solid.PendingPartialWithdrawal
	}{
		{name: "cut then reappend", previous: []*solid.PendingPartialWithdrawal{a, b, c}, updated: []*solid.PendingPartialWithdrawal{c, a}},
		{name: "repeated suffix", previous: []*solid.PendingPartialWithdrawal{a, b, a}, updated: []*solid.PendingPartialWithdrawal{a, b, a, a}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executeTestSSZQueue(t, test.previous, test.updated)
		})
	}
}

func TestApplySSZQueueDiffAllowsTruncateAll(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1}
	b := &solid.PendingPartialWithdrawal{ValidatorIndex: 2}
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{a}, []*solid.PendingPartialWithdrawal{b})
}

func TestApplySSZQueueDiffRejectsOversizedTruncateWithoutMutation(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1}
	b := &solid.PendingPartialWithdrawal{ValidatorIndex: 2}
	queue := pendingWithdrawalList(a, b)
	var diff bytes.Buffer
	if err := binary.Write(&diff, binary.LittleEndian, uint32(queue.Len()+1)); err != nil {
		t.Fatal(err)
	}

	err := ApplySSZQueueDiff(&diff, queue, 0)
	if !errors.Is(err, ErrCorruptQueueDiff) {
		t.Fatalf("expected ErrCorruptQueueDiff, got %v", err)
	}
	assertPendingWithdrawalListEqual(t, pendingWithdrawalList(a, b), queue)
}

func TestSSZQueueDiffExhaustiveMinimalOverlap(t *testing.T) {
	alphabet := []*solid.PendingPartialWithdrawal{
		{ValidatorIndex: 1},
		{ValidatorIndex: 2},
		{ValidatorIndex: 3},
	}
	lists := make([][]*solid.PendingPartialWithdrawal, 0, 121)
	for length := 0; length <= 4; length++ {
		count := 1
		for range length {
			count *= len(alphabet)
		}
		for encoded := 0; encoded < count; encoded++ {
			values := make([]*solid.PendingPartialWithdrawal, length)
			n := encoded
			for i := range values {
				values[i] = alphabet[n%len(alphabet)]
				n /= len(alphabet)
			}
			lists = append(lists, values)
		}
	}

	equal := func(a, b *solid.PendingPartialWithdrawal) bool { return *a == *b }
	for previousIndex, previousValues := range lists {
		for updatedIndex, updatedValues := range lists {
			previous := pendingWithdrawalList(previousValues...)
			updated := pendingWithdrawalList(updatedValues...)
			diff := encodePendingWithdrawalDiff(t, previous, updated, equal)
			result := pendingWithdrawalList(previousValues...)
			if err := ApplySSZQueueDiff(bytes.NewReader(diff), result, 0); err != nil {
				t.Fatalf("previous %d, updated %d: %v", previousIndex, updatedIndex, err)
			}
			assertPendingWithdrawalListEqual(t, updated, result)

			overlap := 0
			for k := 0; k <= min(len(previousValues), len(updatedValues)); k++ {
				if slices.EqualFunc(previousValues[len(previousValues)-k:], updatedValues[:k], equal) {
					overlap = k
				}
			}
			wantLength := 4 + (len(updatedValues)-overlap)*(4+24)
			if len(diff) != wantLength {
				t.Fatalf("previous %d, updated %d: diff length %d, want %d for overlap %d", previousIndex, updatedIndex, len(diff), wantLength, overlap)
			}
		}
	}
}

func TestSSZQueueDiffEqualityCallsAreLinear(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1}
	b := &solid.PendingPartialWithdrawal{ValidatorIndex: 2}
	previousValues := append(slices.Repeat([]*solid.PendingPartialWithdrawal{a}, 1999), b)
	updatedValues := slices.Repeat([]*solid.PendingPartialWithdrawal{a}, 2000)

	calls := 0
	diff := encodePendingWithdrawalDiff(t, pendingWithdrawalList(previousValues...), pendingWithdrawalList(updatedValues...), func(a, b *solid.PendingPartialWithdrawal) bool {
		calls++
		return *a == *b
	})
	if calls > 2*(len(previousValues)+len(updatedValues)) {
		t.Fatalf("equality called %d times for %d total elements", calls, len(previousValues)+len(updatedValues))
	}
	if got, want := len(diff), 4+len(updatedValues)*(4+24); got != want {
		t.Fatalf("diff length %d, want %d", got, want)
	}
}

func TestSSZQueueDiffSharedPointersSkipEquality(t *testing.T) {
	previousValues := make([]*solid.PendingPartialWithdrawal, 1000)
	for i := range previousValues {
		previousValues[i] = &solid.PendingPartialWithdrawal{ValidatorIndex: uint64(i)}
	}
	appended := []*solid.PendingPartialWithdrawal{{ValidatorIndex: 1000}, {ValidatorIndex: 1001}, {ValidatorIndex: 1002}, {ValidatorIndex: 1003}}
	updatedValues := slices.Concat(previousValues[16:], appended)
	previous := pendingWithdrawalList(previousValues...)
	updated := pendingWithdrawalList(updatedValues...)

	calls := 0
	diff := encodePendingWithdrawalDiff(t, previous, updated, func(a, b *solid.PendingPartialWithdrawal) bool {
		calls++
		return *a == *b
	})
	if calls > len(appended) {
		t.Fatalf("equality called %d times, want at most %d", calls, len(appended))
	}
	if got, want := len(diff), 4+len(appended)*(4+24); got != want {
		t.Fatalf("diff length %d, want %d", got, want)
	}
	if err := ApplySSZQueueDiff(bytes.NewReader(diff), previous, 0); err != nil {
		t.Fatal(err)
	}
	assertPendingWithdrawalListEqual(t, updated, previous)
}

func TestSSZQueueDiffDifferentPointersFallBackToValues(t *testing.T) {
	a := &solid.PendingPartialWithdrawal{ValidatorIndex: 1}
	b := &solid.PendingPartialWithdrawal{ValidatorIndex: 2}
	c := &solid.PendingPartialWithdrawal{ValidatorIndex: 3}
	d := &solid.PendingPartialWithdrawal{ValidatorIndex: 4}
	equal := func(a, b *solid.PendingPartialWithdrawal) bool { return *a == *b }

	t.Run("value-equal copy", func(t *testing.T) {
		bCopy := *b
		updated := pendingWithdrawalList(&bCopy, c, d)
		diff := encodePendingWithdrawalDiff(t, pendingWithdrawalList(a, b, c), updated, equal)
		if got, want := len(diff), 4+(4+24); got != want {
			t.Fatalf("diff length %d, want %d", got, want)
		}
		result := pendingWithdrawalList(a, b, c)
		if err := ApplySSZQueueDiff(bytes.NewReader(diff), result, 0); err != nil {
			t.Fatal(err)
		}
		assertPendingWithdrawalListEqual(t, updated, result)
	})

	t.Run("same-list set", func(t *testing.T) {
		list := pendingWithdrawalList(a, b, c)
		encoder := NewSSZQueueEncoder(equal)
		encoder.Initialize(list)
		list.Set(1, &solid.PendingPartialWithdrawal{ValidatorIndex: 2, Amount: 20})
		var diff bytes.Buffer
		if err := encoder.WriteDiff(&diff, list); err != nil {
			t.Fatal(err)
		}
		result := pendingWithdrawalList(a, b, c)
		if err := ApplySSZQueueDiff(&diff, result, 0); err != nil {
			t.Fatal(err)
		}
		assertPendingWithdrawalListEqual(t, list, result)
	})
}

func executeTestSSZQueue(t *testing.T, oldQueue, newQueue []*solid.PendingPartialWithdrawal) {
	// make the lists
	oldList := solid.NewStaticListSSZ[*solid.PendingPartialWithdrawal](1000, 24)
	for _, v := range oldQueue {
		oldList.Append(v)
	}
	newList := solid.NewStaticListSSZ[*solid.PendingPartialWithdrawal](1000, 24)
	for _, v := range newQueue {
		newList.Append(v)
	}

	equalFn := func(a, b *solid.PendingPartialWithdrawal) bool { return *a == *b }
	queueDiffWriter := NewSSZQueueEncoder(equalFn)
	queueDiffWriter.Initialize(oldList)

	var buf bytes.Buffer
	if err := queueDiffWriter.WriteDiff(&buf, newList); err != nil {
		t.Fatal(err)
	}

	if err := ApplySSZQueueDiff(&buf, oldList, 0); err != nil {
		t.Fatal(err)
	}
	// oldList should be equal to newList
	if oldList.Len() != newList.Len() {
		t.Fatalf("oldList.Len() != newList.Len(): %d != %d", oldList.Len(), newList.Len())
	}
	for i := 0; i < oldList.Len(); i++ {
		if !equalFn(oldList.Get(i), newList.Get(i)) {
			t.Fatalf("oldList.Get(i) != newList.Get(i): %v != %v", oldList.Get(i), newList.Get(i))
		}
	}
}

func TestSSZQueue(t *testing.T) {
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 1, ValidatorIndex: 2},
		{Amount: 3, ValidatorIndex: 4},
	}, []*solid.PendingPartialWithdrawal{
		{Amount: 1, ValidatorIndex: 2},
		{Amount: 3, ValidatorIndex: 4},
		{Amount: 5, ValidatorIndex: 6},
	})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 1, ValidatorIndex: 2},
		{Amount: 3, ValidatorIndex: 4},
		{Amount: 5, ValidatorIndex: 6},
	}, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
	})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 1, ValidatorIndex: 2},
		{Amount: 3, ValidatorIndex: 4},
		{Amount: 5, ValidatorIndex: 6},
	}, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
		{Amount: 7, ValidatorIndex: 8},
		{Amount: 8, ValidatorIndex: 9},
		{Amount: 9, ValidatorIndex: 10},
	})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{}, []*solid.PendingPartialWithdrawal{})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
	}, []*solid.PendingPartialWithdrawal{})

	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{}, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
	})

	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{{}}, []*solid.PendingPartialWithdrawal{{}})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
		{Amount: 7, ValidatorIndex: 8},
		{Amount: 8, ValidatorIndex: 9},
		{Amount: 9, ValidatorIndex: 10},
	}, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
		{Amount: 7, ValidatorIndex: 8},
		{Amount: 8, ValidatorIndex: 9},
		{Amount: 9, ValidatorIndex: 10},
	})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
	}, []*solid.PendingPartialWithdrawal{
		{Amount: 6, ValidatorIndex: 7},
		{Amount: 7, ValidatorIndex: 9},
	})
	executeTestSSZQueue(t, []*solid.PendingPartialWithdrawal{
		{Amount: 6, ValidatorIndex: 7},
		{Amount: 7, ValidatorIndex: 9},
	}, []*solid.PendingPartialWithdrawal{
		{Amount: 5, ValidatorIndex: 6},
	})
}
