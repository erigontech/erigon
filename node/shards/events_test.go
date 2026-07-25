package shards

import (
	"testing"

	"github.com/erigontech/erigon/common"
)

func TestOnTransactionValidated_Delivery(t *testing.T) {
	events := NewEvents()

	ch, unsub := events.AddTransactionValidatedSubscription()
	defer unsub()

	txHashes := []common.Hash{{0x01}, {0x02}, {0x03}}
	events.OnTransactionValidated(txHashes)

	got := <-ch
	if len(got) != 3 {
		t.Fatalf("expected 3 hashes, got %d", len(got))
	}
	for i, h := range got {
		if h != txHashes[i] {
			t.Errorf("hash[%d]: got %x, want %x", i, h[:4], txHashes[i][:4])
		}
	}
}

func TestOnTransactionValidated_NoSubscribers(t *testing.T) {
	events := NewEvents()
	// Should not panic with no subscribers.
	events.OnTransactionValidated([]common.Hash{{0xFF}})
}

func TestOnTransactionValidated_Unsubscribe(t *testing.T) {
	events := NewEvents()

	ch, unsub := events.AddTransactionValidatedSubscription()
	unsub()

	// Channel should be closed after unsubscribe.
	_, ok := <-ch
	if ok {
		t.Fatal("expected channel to be closed after unsubscribe")
	}
}

func TestOnTransactionValidated_MultipleSubscribers(t *testing.T) {
	events := NewEvents()

	ch1, unsub1 := events.AddTransactionValidatedSubscription()
	defer unsub1()
	ch2, unsub2 := events.AddTransactionValidatedSubscription()
	defer unsub2()

	txHashes := []common.Hash{{0xAA}, {0xBB}}
	events.OnTransactionValidated(txHashes)

	got1 := <-ch1
	got2 := <-ch2

	if len(got1) != 2 || len(got2) != 2 {
		t.Fatalf("both subscribers should get 2 hashes: got %d and %d", len(got1), len(got2))
	}
	if got1[0] != got2[0] || got1[1] != got2[1] {
		t.Error("subscribers received different data")
	}
}
