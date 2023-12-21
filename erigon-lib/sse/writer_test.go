package sse

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoderWrite(t *testing.T) {
	type testCase struct {
		e string
		i string
		w string
	}
	cases := []testCase{{
		"",
		"foo bar\nbar foo\nwowwwwza\n",
		`data: foo bar
data: bar foo
data: wowwwwza
`}, {
		"hello",
		"there\nfriend",
		`event: hello
data: there
data: friend
`},
	}

	for _, v := range cases {
		buf := &bytes.Buffer{}
		enc := NewWriter(buf)
		err := enc.Header("event", v.e)
		require.NoError(t, err)
		_, err = enc.Write([]byte(v.i))
		require.NoError(t, err)
		require.NoError(t, enc.Flush())
		assert.EqualValues(t, buf.String(), v.w)
	}
}

func TestEncoderWriteData(t *testing.T) {
	type testCase struct {
		e string
		i string
		w string
	}
	cases := []testCase{{
		"",
		"foo bar\nbar foo\nwowwwwza\n",
		`data: foo bar
data: bar foo
data: wowwwwza
`}, {
		"hello",
		"there\nfriend",
		`event: hello
data: there
data: friend
`},
	}

	for _, v := range cases {
		buf := &bytes.Buffer{}
		enc := NewWriter(buf)
		err := enc.Header("event", v.e)
		require.NoError(t, err)
		err = enc.WriteData(strings.NewReader(v.i))
		require.NoError(t, err)
		require.NoError(t, enc.Flush())
		assert.EqualValues(t, v.w, buf.String())
	}
}
