package utils

import (
	"encoding/binary"
	"encoding/hex"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
)

func Uint32ToBytes4(n uint32) (ret [4]byte) {
	binary.BigEndian.PutUint32(ret[:], n)
	return
}

func BytesToBytes4(b []byte) (ret [4]byte) {
	copy(ret[:], b)
	return
}

func BytesToHex(b []byte) string {
	return hex.EncodeToString(b)
}

func DecompressSnappy(data []byte) ([]byte, error) {
	// Decode the snappy
	lenDecoded, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	decodedData := make([]byte, lenDecoded)

	snappy.Decode(decodedData, data)
	return decodedData, nil
}

func CompressSnappy(data []byte) ([]byte, error) {
	// Decode the snappy
	lenDecoded, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	decodedData := make([]byte, lenDecoded)

	snappy.Decode(decodedData, data)
	return decodedData, nil
}

func Uint64ToLE(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func BytesToBytes32(b []byte) (ret [32]byte) {
	copy(ret[:], b)
	return
}

func BytesSliceToBytes32Slice(b [][]byte) (ret [][32]byte) {
	for _, str := range b {
		ret = append(ret, BytesToBytes32(str))
	}
	return
}

func EncodeSSZSnappy(data ssz.Marshaler) ([]byte, error) {
	enc := make([]byte, data.SizeSSZ())
	enc, err := data.MarshalSSZTo(enc[:0])
	if err != nil {
		return nil, err
	}
	return snappy.Encode(make([]byte, data.SizeSSZ()), enc), nil
}

func DecodeSSZSnappy(dst ssz.Unmarshaler, src []byte) error {

	dec, err := snappy.Decode(nil, src)
	if err != nil {
		return err
	}

	err = dst.UnmarshalSSZ(dec)
	if err != nil {
		return err
	}

	return nil
}
