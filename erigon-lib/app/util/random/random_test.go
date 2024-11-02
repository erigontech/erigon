package random_test

import (
	"testing"

	"github.com/erigontech/erigon-lib/app/util/random"
	"github.com/stretchr/testify/assert"
)

func TestRandom(t *testing.T) {
	assert.Equal(t, len(random.RandomBytes(60)), 60)
	assert.Equal(t, len(random.RandomString(160)), 160)
}
