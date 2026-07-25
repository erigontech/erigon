package enr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBTGetSet(t *testing.T) {
	var r Record
	r.Set(BT(42069))

	var bt BT
	require.NoError(t, r.Load(&bt))
	assert.Equal(t, BT(42069), bt)
}

func TestBTENRKey(t *testing.T) {
	assert.Equal(t, "bt", BT(0).ENRKey())
}

func TestBTNotFound(t *testing.T) {
	var r Record
	var bt BT
	err := r.Load(&bt)
	require.Error(t, err)
	assert.True(t, IsNotFound(err))
}

func TestBTFitsInENR(t *testing.T) {
	var r Record
	r.Set(IDv4)
	r.Set(IPv4{192, 168, 1, 1})
	r.Set(UDP(30303))
	r.Set(TCP(30303))
	r.Set(ChainToml{
		AuthoritativeBlocks: ^uint64(0),
		KnownBlocks:         ^uint64(0),
		InfoHash:            [20]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	})
	r.Set(BT(42069))

	assert.Less(t, r.Size(), uint64(SizeLimit), "BT + ChainToml should fit within ENR size limit")
}
