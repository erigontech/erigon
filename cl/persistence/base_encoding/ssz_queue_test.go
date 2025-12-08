package base_encoding

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

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
