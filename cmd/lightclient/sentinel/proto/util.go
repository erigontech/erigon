package proto

import (
	"errors"
	"io"
)

func ReadUvarint(r io.Reader) (uint64, uint64, error) {
	var x uint64
	var s uint
	var total uint64
	bs := [1]byte{}
	for i := 0; i < 10; i++ {
		_, err := r.Read(bs[:])
		if err != nil {
			return x, total, err
		}
		b := bs[0]
		total = total + 1
		if b < 0x80 {
			if i == 10-1 && b > 1 {
				return x, total, errors.New("readUvarint: overflow")
			}
			return x | uint64(b)<<s, total, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, total, errors.New("readUvarint: overflow")
}
