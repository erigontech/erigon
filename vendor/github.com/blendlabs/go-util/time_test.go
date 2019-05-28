package util

import (
	"testing"
	"time"

	assert "github.com/blendlabs/go-assert"
)

func TestTimeUnixMillis(t *testing.T) {
	assert := assert.New(t)

	zero := Time.UnixMillis(time.Unix(0, 0))
	assert.Zero(zero)

	sample := Time.UnixMillis(time.Date(2015, 03, 07, 16, 0, 0, 0, time.UTC))
	assert.Equal(1425744000000, sample)
}

func TestTimeFromMillis(t *testing.T) {
	assert := assert.New(t)

	ts := time.Date(2015, 03, 07, 16, 0, 0, 0, time.UTC)
	millis := Time.UnixMillis(ts)
	ts2 := Time.FromMillis(millis)
	assert.Equal(ts, ts2)
}
