package hex

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodeBig(t *testing.T) {
	b := big.NewInt(math.MaxInt64)
	e := EncodeBig(b)
	d := DecodeBig(e)
	assert.Equal(t, b.Uint64(), d.Uint64())
}
