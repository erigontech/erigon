package compress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	expectWord := []byte("hi")
	_, vv := EncodeSnappyIfNeed(nil, expectWord, true)
	var buf []byte
	buf, word, err := DecodeSnappyIfNeed(buf, vv, true)
	require.NoError(t, err)
	require.Equal(t, expectWord, word)
	buf, word, err = DecodeSnappyIfNeed(buf, vv, true)
	require.NoError(t, err)
	require.Equal(t, expectWord, word)
	buf, word, err = DecodeSnappyIfNeed(buf, vv, true)
	require.NoError(t, err)
	require.Equal(t, expectWord, word)
}
