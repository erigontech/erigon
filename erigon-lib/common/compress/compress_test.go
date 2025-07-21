package compress

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	expectWord := []byte("hi")
	var bbb, vv []byte
	bbb, vv = EncodeZstdIfNeed(bbb[:0], expectWord, true)
	fmt.Printf("bbb: %d, vv: %d\n", len(bbb), len(vv))
	bbb, vv = EncodeZstdIfNeed(bbb[:0], expectWord, true)
	fmt.Printf("bbb: %d, vv: %d\n", len(bbb), len(vv))
	bbb, vv = EncodeZstdIfNeed(bbb[:0], expectWord, true)
	fmt.Printf("bbb: %d, vv: %d\n", len(bbb), len(vv))
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
