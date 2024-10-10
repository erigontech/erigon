package base_encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRabbit(t *testing.T) {
	list := []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 23, 90}
	var w bytes.Buffer
	if err := WriteRabbits(list, &w); err != nil {
		t.Fatal(err)
	}
	var out []uint64
	out, err := ReadRabbits(out, &w)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, list, out)
}
