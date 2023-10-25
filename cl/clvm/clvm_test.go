package clvm

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {

	payload := `add(010340,283283) sub(23828324,4333)

max(ababab,bababa)
`
	dec := NewDecoder(strings.NewReader(payload))
	require.True(t, dec.Scan())
	cycle := dec.Cycle()
	ok, err := cycle.Step()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, "add", string(cycle.Pc().Opcode()))
	ok, err = cycle.Step()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, "sub", string(cycle.Pc().Opcode()))
}

func TestEncoder(t *testing.T) {

	buf := &bytes.Buffer{}

	enc := NewEncoder(buf)
	enc.WriteCycle(NewInstruction([]byte("foo"), []byte("bar")))
	enc.WriteCycle()
	enc.WriteCycle(NewInstruction([]byte("bar"), []byte("foo")))
	expected := `foo(626172)

bar(666f6f)
`
	require.EqualValues(t, expected, string(buf.Bytes()))

}
