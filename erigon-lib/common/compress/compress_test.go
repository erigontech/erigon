package compress

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	_, vv := EncodeSnappyIfNeed(nil, []byte("hi"), true)
	_, word, err := DecodeSnappyIfNeed(nil, vv, true)
	require.NoError(t, err)
	fmt.Printf("%s\n", word)
}
