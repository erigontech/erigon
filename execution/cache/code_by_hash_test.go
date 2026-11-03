package cache

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
)

func TestStateCache_PutCodeByHashHonoursUnwind(t *testing.T) {
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	t.Cleanup(c.Close)
	kept, dropped := makeCode(1), makeCode(2)
	keptHash, droppedHash := crypto.Keccak256Hash(kept), crypto.Keccak256Hash(dropped)

	c.PutCodeByHash(keptHash[:], kept, 30)
	c.PutCodeByHash(droppedHash[:], dropped, 50)
	got, ok := c.GetCodeByHash(droppedHash[:])
	require.True(t, ok)
	require.Equal(t, dropped, got)

	c.Applier().Unwind(40)

	_, ok = c.GetCodeByHash(droppedHash[:])
	require.False(t, ok, "code read at or above the unwind point must not survive it")
	got, ok = c.GetCodeByHash(keptHash[:])
	require.True(t, ok)
	require.Equal(t, kept, got)
}
