package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPowerOf2(t *testing.T) {
	assert := assert.New(t)
	type T struct {
		in     uint
		expect uint
	}

	cases := []T{
		{1<<14 - 1, 13},
		{1<<14 + 1, 14},
		{1 << 14, 14},
		{1 << 8, 8},
		{1<<8 + 1, 8},
		{1<<18 + 1, 18},
		{MaxPool + 1, MaxPoolPow},
		{MaxPool, MaxPoolPow},
		{MinPool / 2, MinPoolPow},
		{MinPool - 1, MinPoolPow},
		{MinPool, MinPoolPow},
		{1, MinPoolPow},
	}

	for _, c := range cases {
		assert.Equal(int(c.expect), poolIdx(c.in), int(c.in))
	}
}
