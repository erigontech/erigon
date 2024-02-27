package sais

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSais(t *testing.T) {
	data := []byte{4, 5, 6, 4, 5, 6, 4, 5, 6}
	sa := make([]int32, len(data))
	err := Sais(data, sa)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []int32{6, 3, 0, 7, 4, 1, 8, 5, 2}, sa)
}
