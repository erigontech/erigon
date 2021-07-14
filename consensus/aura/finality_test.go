package aura

import (
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
)

func TestRollingFinality(t *testing.T) {
	t.Run("RejectsUnknownSigners", func(t *testing.T) {
		f := NewRollingFinality([]common.Address{{1}, {2}, {3}})
		_, err := f.push(common.Hash{}, 0, []common.Address{{0}, {4}})
		assert.Error(t, err)
		_, err = f.push(common.Hash{}, 0, []common.Address{{0}, {1}, {4}})
		assert.Error(t, err)
	})
	t.Run("FinalizeMultiple", func(t *testing.T) {
		signers := []common.Address{{0}, {1}, {2}, {3}, {4}, {5}}
		f := NewRollingFinality(signers)
		// 3 / 6 signers is < 51% so no finality.
		for i := 0; i < 6; i++ {
			l, err := f.push(common.Hash{byte(i)}, uint64(i%3), []common.Address{signers[i%3]})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(l))
		}
		// after pushing a block signed by a fourth validator, the first four
		// blocks of the unverified chain become verified.
		l, err := f.push(common.Hash{byte(6)}, 6, []common.Address{signers[4]})
		assert.NoError(t, err)
		for i := uint64(0); i < 4; i++ {
			assert.Equal(t, common.Hash{byte(i)}, l[i].hash)
		}
		assert.Equal(t, 4, len(l))
	})
	t.Run("FromAncestry", func(t *testing.T) {
		signers := []common.Address{{0}, {1}, {2}, {3}, {4}, {5}}
		f := NewRollingFinality(signers)
		i := 12
		get := func(hash common.Hash) ([]common.Address, common.Hash, common.Hash, uint64, bool) {
			i--
			if i == -1 {
				return nil, common.Hash{}, common.Hash{}, 0, false
			}
			return []common.Address{signers[i%6]}, common.Hash{byte(i)}, common.Hash{byte(i - 1)}, uint64(i), true
		}
		err := f.buildAncestrySubChain(get, common.Hash{11}, common.Hash{99})
		assert.NoError(t, err)
		assert.Equal(t, 3, f.headers.l.Len())
		assert.Equal(t, common.Hash{11}, *f.lastPushed)
	})
	t.Run("FromAncestryMultipleSigners", func(t *testing.T) {
		signers := []common.Address{{0}, {1}, {2}, {3}, {4}, {5}}
		f := NewRollingFinality(signers)
		i := 12
		get := func(hash common.Hash) ([]common.Address, common.Hash, common.Hash, uint64, bool) {
			i--
			if i == -1 {
				return nil, common.Hash{}, common.Hash{}, 0, false
			}
			return []common.Address{signers[i%6], signers[(i+1)%6], signers[(i+2)%6]}, common.Hash{byte(i)}, common.Hash{byte(i - 1)}, uint64(i), true
		}
		err := f.buildAncestrySubChain(get, common.Hash{11}, common.Hash{99})
		assert.NoError(t, err)

		// only the last hash has < 51% of authorities' signatures
		assert.Equal(t, 1, f.headers.l.Len())
		assert.Equal(t, common.Hash{11}, f.headers.Front().hash)
		assert.Equal(t, common.Hash{11}, *f.lastPushed)
	})

}
