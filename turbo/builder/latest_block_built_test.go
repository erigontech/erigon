package builder

import (
	"testing"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
)

func TestLatestBlockBuilt(t *testing.T) {
	t.Parallel()
	s := NewLatestBlockBuiltStore()
	b := types.NewBlockWithHeader(&types.Header{})
	s.AddBlockBuilt(b)
	assert.Equal(t, b.Header(), s.BlockBuilt().Header())
}
