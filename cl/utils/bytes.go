/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package utils

import (
	"encoding/binary"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
)

func Uint32ToBytes4(n uint32) (ret [4]byte) {
	binary.BigEndian.PutUint32(ret[:], n)
	return
}

func Bytes4ToUint32(bytes4 [4]byte) uint32 {
	return binary.BigEndian.Uint32(bytes4[:])
}

func BytesToBytes4(b []byte) (ret [4]byte) {
	copy(ret[:], b)
	return
}

func Uint64ToLE(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func DecompressSnappy(data []byte) ([]byte, error) {
	// Decode the snappy
	lenDecoded, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	decodedData := make([]byte, lenDecoded)

	return snappy.Decode(decodedData, data)
}

func CompressSnappy(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func EncodeSSZSnappy(data ssz_utils.Marshaler) ([]byte, error) {
	var (
		enc = make([]byte, 0, data.EncodingSizeSSZ())
		err error
	)
	enc, err = data.EncodeSSZ(enc)
	if err != nil {
		return nil, err
	}

	return snappy.Encode(nil, enc), nil
}

func DecodeSSZSnappy(dst ssz_utils.Unmarshaler, src []byte) error {
	dec, err := snappy.Decode(nil, src)
	if err != nil {
		return err
	}

	err = dst.DecodeSSZ(dec)
	if err != nil {
		return err
	}

	return nil
}

func DecodeSSZSnappyWithVersion(dst ssz_utils.Unmarshaler, src []byte, version int) error {
	dec, err := snappy.Decode(nil, src)
	if err != nil {
		return err
	}

	err = dst.DecodeSSZWithVersion(dec, version)
	if err != nil {
		return err
	}

	return nil
}

func CompressZstd(b []byte) []byte {
	wr, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	return wr.EncodeAll(b, nil)
}

func DecompressZstd(b []byte) ([]byte, error) {
	r, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
	return r.DecodeAll(b, nil)
}

// Check if it is sorted and check if there are duplicates. O(N) complexity.
func IsSliceSortedSet(vals []uint64) bool {
	for i := 0; i < len(vals)-1; i++ {
		if vals[i] >= vals[i+1] {
			return false
		}
	}
	return true
}
