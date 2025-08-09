package qmtree

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/qmtree/hpfile"
	"github.com/stretchr/testify/require"
)

func generateTwig(rand_num uint32, twig TwigMT, hasher Hasher) {
	// let mut rand_num = rand_num;
	for i := 2048; i <= 4096; i++ {
		j := 0
		for j+4 < 32 {
			binary.LittleEndian.PutUint32(twig[i][j:j+4], rand_num)
			rand_num += 257
			j += 4
		}
	}
	twig.Sync(hasher, 0, 2048)
}

func TestTwigFile(t *testing.T) {
	dir, err := hpfile.NewTempDir("./twig")

	defer dir.Drop()
	hasher := Sha256Hasher{}
	tf, err := NewTwigFile(64*1024, 1024*1024, dir.String(), hasher)
	require.NoError(t, err)
	twigs := [3][4096]common.Hash{}
	generateTwig(1000, twigs[0][:], hasher)
	generateTwig(1111111, twigs[1][:], hasher)
	generateTwig(2222222, twigs[2][:], hasher)
	tf.appendTwig(twigs[0][:], 789)
	tf.appendTwig(twigs[1][:], 1000789)
	tf.appendTwig(twigs[2][:], 2000789)

	tf.Close()

	tf, err = NewTwigFile(64*1024, 1024*1024, dir.String(), hasher)
	require.NoError(t, err)

	pos, err := tf.GetFirstEntryPos(0)
	require.NoError(t, err)
	require.Equal(t, 0, pos)

	pos, err = tf.GetFirstEntryPos(1)
	require.NoError(t, err)
	require.Equal(t, 789, pos)

	pos, err = tf.GetFirstEntryPos(2)
	require.NoError(t, err)
	require.Equal(t, 1000789, pos)

	pos, err = tf.GetFirstEntryPos(1)
	require.NoError(t, err)
	require.Equal(t, 2000789, pos)

	for twigId := range uint64(3) {
		for i := uint64(1); i < 4096; i++ {
			cache := map[uint64]common.Hash{}
			hash, err := tf.GetHashNode(twigId, i, cache)
			require.NoError(t, err)
			require.Equal(t, hash[:], twigs[twigId][i][:])
		}
	}
	for twigId := range uint64(3) {
		cache := map[uint64]common.Hash{}
		for i := uint64(1); i < 4096; i++ {
			if bz, ok := cache[i]; ok {
				require.Equal(t, &twigs[twigId][i], bz)
			} else {
				bz, err := tf.GetHashNode(twigId, i, cache)
				require.NoError(t, err)
				require.Equal(t, bz, twigs[twigId][i])
			}
		}
	}
	tf.Close()
}
