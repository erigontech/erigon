package etl

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/ugorji/go/codec"
	"gotest.tools/assert"
)

func TestWriteAndReadBufferEntry(t *testing.T) {

	buffer := bytes.NewBuffer(make([]byte, 0))
	encoder := codec.NewEncoder(buffer, &cbor)

	keys := make([]string, 100)
	vals := make([]string, 100)

	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		vals[i] = fmt.Sprintf("value-%d", i)
	}

	for i := range keys {
		if err := writeToDisk(encoder, []byte(keys[i]), []byte(vals[i])); err != nil {
			t.Error(err)
		}
	}

	bb := buffer.Bytes()

	readBuffer := bytes.NewReader(bb)

	decoder := codec.NewDecoder(readBuffer, &cbor)

	for i := range keys {
		k, v, err := readElementFromDisk(decoder)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, keys[i], string(k))
		assert.Equal(t, vals[i], string(v))
	}

	_, _, err := readElementFromDisk(decoder)
	assert.Equal(t, io.EOF, err)
}
