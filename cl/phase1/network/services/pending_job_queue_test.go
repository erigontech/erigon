package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPendingJobQueueLazyEnqueueSkipsKeyBuildAtCapacity(t *testing.T) {
	queue := newPendingJobQueue[int, string](1, time.Minute, time.Millisecond, nil, nil)
	queue.count.Store(queue.capacity)

	keyBuilt := false
	err := queue.enqueueLazy("message", func() (int, error) {
		keyBuilt = true
		return 1, nil
	})

	require.NoError(t, err)
	require.False(t, keyBuilt)
	require.Equal(t, queue.capacity, queue.count.Load())
}
