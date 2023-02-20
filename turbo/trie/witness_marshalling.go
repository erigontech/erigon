package trie

import (
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ugorji/go/codec"
)

var cbor codec.CborHandle

// OperatorMarshaller provides all needed primitives to read witness operators from a serialized form.
type OperatorUnmarshaller struct {
	reader  io.Reader
	decoder *codec.Decoder
}

func NewOperatorUnmarshaller(r io.Reader) *OperatorUnmarshaller {
	return &OperatorUnmarshaller{r, codec.NewDecoder(r, &cbor)}
}

func (l *OperatorUnmarshaller) ReadByteArray() ([]byte, error) {
	var buffer []byte
	err := l.decoder.Decode(&buffer)
	if err != nil {
		return []byte{}, err
	}

	return buffer, nil
}

func (l *OperatorUnmarshaller) ReadHash() (libcommon.Hash, error) {
	var hash libcommon.Hash
	bytesRead, err := l.reader.Read(hash[:])
	if err != nil {
		return hash, err
	}
	if bytesRead != len(hash) {
		return hash, fmt.Errorf("error while reading hash from input. expected to read %d bytes, read only %d", len(hash), bytesRead)
	}
	return hash, nil
}

func (l *OperatorUnmarshaller) ReadUint32() (uint32, error) {
	var value uint32
	if err := l.decoder.Decode(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func (l *OperatorUnmarshaller) ReadByte() (byte, error) {
	values := make([]byte, 1)
	bytesRead, err := l.reader.Read(values)
	if err != nil {
		return 0, err
	}
	if bytesRead < 1 {
		return 0, fmt.Errorf("could not read a byte from a reader")
	}
	return values[0], nil
}

func (l *OperatorUnmarshaller) ReadKey() ([]byte, error) {
	b, err := l.ReadByteArray()
	if err != nil {
		return nil, err
	}
	return keyBytesToNibbles(b), nil
}

func (l *OperatorUnmarshaller) ReadUInt64() (uint64, error) {
	var value uint64
	err := l.decoder.Decode(&value)
	if err != nil {
		return 0, err
	}
	return value, nil

}

// OperatorMarshaller is responsible for encoding operators to a stream
// and collecting stats
// IMPORTANT: not thread-safe! use from a single thread only
type OperatorMarshaller struct {
	currentColumn StatsColumn
	encoder       *codec.Encoder
	w             io.Writer
	stats         map[StatsColumn]uint64
	total         uint64
}

func NewOperatorMarshaller(w io.Writer) *OperatorMarshaller {
	marshaller := &OperatorMarshaller{w: w, stats: make(map[StatsColumn]uint64)}
	// so we can collect stats
	marshaller.encoder = codec.NewEncoder(marshaller, &cbor)
	return marshaller
}

func (w *OperatorMarshaller) WriteOpCode(opcode OperatorKindCode) error {
	w.WithColumn(ColumnStructure)
	_, err := w.Write([]byte{byte(opcode)})
	return err
}

func (w *OperatorMarshaller) WriteKey(keyNibbles []byte) error {
	w.WithColumn(ColumnLeafKeys)
	return w.encoder.Encode(keyNibblesToBytes(keyNibbles))
}

func (w *OperatorMarshaller) WriteByteValue(value byte) error {
	w.WithColumn(ColumnLeafValues)
	_, err := w.Write([]byte{value})
	return err
}

func (w *OperatorMarshaller) WriteUint64Value(value uint64) error {
	w.WithColumn(ColumnLeafValues)
	return w.encoder.Encode(value)
}

func (w *OperatorMarshaller) WriteByteArrayValue(value []byte) error {
	w.WithColumn(ColumnLeafValues)
	return w.encoder.Encode(value)
}

func (w *OperatorMarshaller) WriteCode(value []byte) error {
	w.WithColumn(ColumnCodes)
	return w.encoder.Encode(value)
}

func (w *OperatorMarshaller) WriteHash(hash libcommon.Hash) error {
	w.WithColumn(ColumnHashes)
	_, err := w.Write(hash[:])
	return err
}

func (w *OperatorMarshaller) Write(p []byte) (int, error) {
	val := w.stats[w.currentColumn]

	written, err := w.w.Write(p)

	val += uint64(written)
	w.total += uint64(written)

	w.stats[w.currentColumn] = val

	return written, err
}

func (w *OperatorMarshaller) WithColumn(column StatsColumn) *OperatorMarshaller {
	w.currentColumn = column
	return w
}

func (w *OperatorMarshaller) GetStats() *BlockWitnessStats {
	return &BlockWitnessStats{
		witnessSize: w.total,
		stats:       w.stats,
	}
}

func keyNibblesToBytes(nibbles []byte) []byte {
	if len(nibbles) < 1 {
		return []byte{}
	}
	if len(nibbles) < 2 {
		return nibbles
	}
	hasTerminator := false
	if nibbles[len(nibbles)-1] == 0x10 {
		nibbles = nibbles[:len(nibbles)-1]
		hasTerminator = true
	}

	targetLen := len(nibbles)/2 + len(nibbles)%2 + 1

	result := make([]byte, targetLen)
	nibbleIndex := 0
	result[0] = byte(len(nibbles) % 2) // parity bit
	for i := 1; i < len(result); i++ {
		result[i] = nibbles[nibbleIndex] * 16
		nibbleIndex++
		if nibbleIndex < len(nibbles) {
			result[i] += nibbles[nibbleIndex]
			nibbleIndex++
		}
	}
	if hasTerminator {
		result[0] |= 1 << 1
	}

	return result
}

func keyBytesToNibbles(b []byte) []byte {
	if len(b) < 1 {
		return []byte{}
	}
	if len(b) < 2 {
		return b
	}

	hasTerminator := b[0]&(1<<1) != 0

	targetLen := (len(b)-1)*2 - int(b[0]&1)

	nibbles := make([]byte, targetLen)

	nibbleIndex := 0
	for i := 1; i < len(b); i++ {
		nibbles[nibbleIndex] = b[i] / 16
		nibbleIndex++
		if nibbleIndex < len(nibbles) {
			nibbles[nibbleIndex] = b[i] % 16
			nibbleIndex++
		}
	}
	if hasTerminator {
		return append(nibbles, 0x10)
	}
	return nibbles
}
