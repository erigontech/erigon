package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	err := Sleep(ctx, 100*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)

	eg := errgroup.Group{}
	eg.Go(func() error {
		return Sleep(ctx, time.Minute)
	})
	cancel()
	err = eg.Wait()
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), time.Minute)
}
