package sse

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoderSimple(t *testing.T) {
	type testCase struct {
		xs []*Packet
		w  string
	}
	cases := []testCase{{
		[]*Packet{
			{Event: "hello", Data: strings.NewReader("some data")},
			{Data: strings.NewReader("some other data with no event header")},
		},
		"event: hello\ndata: some data\n\ndata: some other data with no event header\n",
	},
		{
			[]*Packet{
				{Event: "hello", Data: strings.NewReader("some \n funky\r\n data\r")},
				{Data: strings.NewReader("some other data with an id"), ID: ID("dogs")},
			},
			"event: hello\ndata: some \ndata:  funky\r\ndata:  data\r\ndata: some other data with an id\nid: dogs\n",
		},
	}
	for _, v := range cases {
		buf := &bytes.Buffer{}
		enc := NewEncoder(buf)
		for _, p := range v.xs {
			require.NoError(t, enc.Encode(p))
		}
		assert.EqualValues(t, v.w, buf.String())
	}
}
