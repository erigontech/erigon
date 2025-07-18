package compress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	expectWord := []byte("hi")
	_, vv := EncodeZstdIfNeed(nil, expectWord, true)
	var buf []byte
	buf, word, err := DecodeZstdIfNeed(buf, vv, true)
	require.NoError(t, err)
	require.Equal(t, expectWord, word)
	buf, word, err = DecodeZstdIfNeed(buf, vv, true)
	require.NoError(t, err)
	require.Equal(t, expectWord, word)
	buf, word, err = DecodeZstdIfNeed(buf, vv, true)
	require.NoError(t, err)
	require.Equal(t, expectWord, word)

	_ = buf
}
