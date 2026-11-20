package rpchelper

import (
	"context"
	"testing"
	"time"
)

func TestHeadBarrierReleasesLogsWhenEveryHeadIsWritten(t *testing.T) {
	var b headBarrier
	bh := b.get(7)
	bh.pending.Add(2)
	bh.Written() // the header delivery
	bh.Written()
	go bh.Written()
	start := time.Now()
	b.wait(context.Background(), 7)
	if time.Since(start) >= headsFirstWait {
		t.Fatalf("waited for the timeout although every head was written")
	}
}
