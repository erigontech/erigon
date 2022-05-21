package iavl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyFormatBytes(t *testing.T) {
	kf := NewKeyFormat(byte('e'), 8, 8, 8)
	assert.Equal(t, []byte{'e', 0, 0, 0, 0, 0, 1, 2, 3}, kf.KeyBytes([]byte{1, 2, 3}))
	assert.Equal(t, []byte{'e', 1, 2, 3, 4, 5, 6, 7, 8}, kf.KeyBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	assert.Equal(t, []byte{'e', 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 1, 1, 2, 2, 3, 3},
		kf.KeyBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 1, 2, 2, 3, 3}))
	assert.Equal(t, []byte{'e'}, kf.KeyBytes())
}

func TestKeyFormat(t *testing.T) {
	kf := NewKeyFormat(byte('e'), 8, 8, 8)
	key := []byte{'e', 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 1, 144}
	var a, b, c int64 = 100, 200, 400
	assert.Equal(t, key, kf.Key(a, b, c))

	var ao, bo, co = new(int64), new(int64), new(int64)
	kf.Scan(key, ao, bo, co)
	assert.Equal(t, a, *ao)
	assert.Equal(t, b, *bo)
	assert.Equal(t, c, *co)

	bs := new([]byte)
	kf.Scan(key, ao, bo, bs)
	assert.Equal(t, a, *ao)
	assert.Equal(t, b, *bo)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 1, 144}, *bs)

	assert.Equal(t, []byte{'e', 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 200}, kf.Key(a, b))
}

func TestNegativeKeys(t *testing.T) {
	kf := NewKeyFormat(byte('e'), 8, 8)

	var a, b int64 = -100, -200
	// One's complement plus one
	key := []byte{'e',
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, byte(0xff + a + 1),
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, byte(0xff + b + 1)}
	assert.Equal(t, key, kf.Key(a, b))

	var ao, bo = new(int64), new(int64)
	kf.Scan(key, ao, bo)
	assert.Equal(t, a, *ao)
	assert.Equal(t, b, *bo)
}

func TestOverflow(t *testing.T) {
	kf := NewKeyFormat(byte('o'), 8, 8)

	var a int64 = 1 << 62
	var b uint64 = 1 << 63
	key := []byte{'o',
		0x40, 0, 0, 0, 0, 0, 0, 0,
		0x80, 0, 0, 0, 0, 0, 0, 0,
	}
	assert.Equal(t, key, kf.Key(a, b))

	var ao, bo = new(int64), new(int64)
	kf.Scan(key, ao, bo)
	assert.Equal(t, a, *ao)
	assert.Equal(t, int64(b), *bo)
}
