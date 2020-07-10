package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolPutIdx(t *testing.T) {
	assert := assert.New(t)
	type T struct {
		in     uint
		expect uint
	}

	cases := []T{
		{1<<18 + 1, 7},
		{1<<14 - 1, 4},
		{1<<14 + 1, 5},
		{1 << 14, 5},
		{1 << 8, 2},
		{1<<8 + 1, 2},
		{1 << 7, 1},
		{1<<7 - 1, 1},
		{1<<7 + 1, 1},
		{1<<MaxPoolPow + 1, (MaxPoolPow - MinPoolPow) / 2},
		{1 << MaxPoolPow, (MaxPoolPow - MinPoolPow) / 2},
		{1 << MinPoolPow / 2, 0},
		{1<<MinPoolPow - 1, 0},
		{1 << MinPoolPow, 0},
		{1, 0},
		{72, 1},
		{63, 0},
		{64, 1},
		{65, 1},
	}

	for _, c := range cases {
		assert.Equal(int(c.expect), poolPutIdx(c.in), int(c.in))
	}
}

func TestPoolGetIdx(t *testing.T) {
	assert := assert.New(t)
	type T struct {
		in     uint
		expect uint
	}

	cases := []T{
		{1<<18 + 1, 8},
		{1 << 14, 5},
		{1<<14 + 1, 6},
		{1<<14 - 1, 5},
		{1 << 8, 2},
		{1<<8 + 1, 3},
		{1 << 7, 2},
		{1<<7 - 1, 2},
		{1<<7 + 1, 2},
		{1<<MaxPoolPow + 1, (MaxPoolPow - MinPoolPow) / 2},
		{1 << MaxPoolPow, (MaxPoolPow - MinPoolPow) / 2},
		{1 << MinPoolPow / 2, 0},
		{1<<MinPoolPow - 1, 0},
		{1 << MinPoolPow, 0},
		{1, 0},
		{72, 2},
		{63, 1},
		{64, 1},
		{65, 2},
	}

	for _, c := range cases {
		assert.Equal(int(c.expect), poolGetIdx(c.in), int(c.in))
	}
}
