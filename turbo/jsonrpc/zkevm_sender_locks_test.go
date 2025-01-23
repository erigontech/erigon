package jsonrpc

import (
	"sync"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
)

func Test_SenderLocks(t *testing.T) {
	sl := NewSenderLock()
	addr := common.HexToAddress("0x1")

	// ensure 0 to start
	lock := sl.GetLock(addr)
	if lock != 0 {
		t.Fatalf("expected lock to be 0, got %d", lock)
	}

	// add a lock and check it shows
	sl.AddLock(addr)
	lock = sl.GetLock(addr)
	if lock != 1 {
		t.Fatalf("expected lock to be 1, got %d", lock)
	}

	// add another lock and check it shows
	sl.AddLock(addr)
	lock = sl.GetLock(addr)
	if lock != 2 {
		t.Fatalf("expected lock to be 2, got %d", lock)
	}

	// now release one and check it shows
	sl.ReleaseLock(addr)
	lock = sl.GetLock(addr)
	if lock != 1 {
		t.Fatalf("expected lock to be 1, got %d", lock)
	}

	// now release the last one and check it shows
	sl.ReleaseLock(addr)
	lock = sl.GetLock(addr)
	if lock != 0 {
		t.Fatalf("expected lock to be 0, got %d", lock)
	}
	if len(sl.locks) != 0 {
		t.Fatalf("expected lock to be 0, got %d", len(sl.locks))
	}
}

func Test_SenderLocks_Concurrency(t *testing.T) {
	sl := NewSenderLock()
	addr := common.HexToAddress("0x1")

	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			sl.AddLock(addr)
		}()
	}
	wg.Wait()

	lock := sl.GetLock(addr)
	if lock != 1000 {
		t.Fatalf("expected lock to be 1000, got %d", lock)
	}

	// now release all the locks concurrently
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			sl.ReleaseLock(addr)
		}()
	}
	wg.Wait()

	lock = sl.GetLock(addr)
	if lock != 0 {
		t.Fatalf("expected lock to be 0, got %d", lock)
	}
	if len(sl.locks) != 0 {
		t.Fatalf("expected lock to be 0, got %d", len(sl.locks))
	}
}

func Test_SenderLocks_MultipleAccounts(t *testing.T) {
	sl := NewSenderLock()
	addr1 := common.HexToAddress("0x1")
	addr2 := common.HexToAddress("0x2")

	sl.AddLock(addr1)
	sl.AddLock(addr2)

	lock1 := sl.GetLock(addr1)
	lock2 := sl.GetLock(addr2)
	if lock1 != 1 {
		t.Fatalf("expected lock to be 1, got %d", lock1)
	}
	if lock2 != 1 {
		t.Fatalf("expected lock to be 1, got %d", lock2)
	}

	sl.ReleaseLock(addr1)

	lock1 = sl.GetLock(addr1)
	lock2 = sl.GetLock(addr2)
	if lock1 != 0 {
		t.Fatalf("expected lock to be 1, got %d", lock1)
	}
	if lock2 != 1 {
		t.Fatalf("expected lock to be 1, got %d", lock2)
	}

	sl.ReleaseLock(addr2)

	lock1 = sl.GetLock(addr1)
	lock2 = sl.GetLock(addr2)
	if lock1 != 0 {
		t.Fatalf("expected lock to be 1, got %d", lock1)
	}
	if lock2 != 0 {
		t.Fatalf("expected lock to be 1, got %d", lock2)
	}
}

func Test_SenderLocks_ReleaseWhenEmpty(t *testing.T) {
	sl := NewSenderLock()
	addr := common.HexToAddress("0x1")

	sl.ReleaseLock(addr)

	lock := sl.GetLock(addr)
	if lock != 0 {
		t.Fatalf("expected lock to be 0, got %d", lock)
	}
}
