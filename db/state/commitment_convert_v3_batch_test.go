package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

func TestConvertedBatchEmitCapsBuffer(t *testing.T) {
	var batch convertedBatch
	require.NoError(t, batch.emit([]byte{1}, []byte{2}, v3.LegacyDirect))
	require.ErrorIs(t, batch.emit(bytes.Repeat([]byte{3}, commitmentV3BatchBytes), nil, v3.LegacyDirect), errConvertedBatchFull)
	require.LessOrEqual(t, len(batch.buf), commitmentV3BatchBytes)
}
