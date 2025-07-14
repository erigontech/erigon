// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package ssz_snappy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/golang/snappy"

	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var writerPool = sync.Pool{
	New: func() any {
		return snappy.NewBufferedWriter(nil)
	},
}

func EncodeAndWrite(w io.Writer, val ssz.Marshaler, prefix ...byte) error {
	enc := make([]byte, 0, val.EncodingSizeSSZ())
	var err error
	enc, err = val.EncodeSSZ(enc)
	if err != nil {
		return err
	}
	// create prefix for length of packet
	lengthBuf := make([]byte, 10)
	vin := binary.PutUvarint(lengthBuf, uint64(len(enc)))

	// Create writer size
	wr := bufio.NewWriterSize(w, 10+len(enc))
	defer wr.Flush()
	// Write length of packet
	wr.Write(prefix)
	wr.Write(lengthBuf[:vin])
	// start using streamed snappy compression
	sw, _ := writerPool.Get().(*snappy.Writer)
	sw.Reset(wr)
	defer func() {
		sw.Flush()
		writerPool.Put(sw)
	}()
	// Marshall and snap it
	_, err = sw.Write(enc)
	return err
}

func DecodeAndRead(r io.Reader, val ssz.EncodableSSZ, b *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock) error {
	var forkDigest [4]byte
	// TODO(issues/5884): assert the fork digest matches the expectation for
	// a specific configuration.
	if _, err := r.Read(forkDigest[:]); err != nil {
		return err
	}
	version, err := ethClock.StateVersionByForkDigest(forkDigest)
	if err != nil {
		return err
	}
	return DecodeAndReadNoForkDigest(r, val, version)
}

func DecodeAndReadNoForkDigest(r io.Reader, val ssz.EncodableSSZ, version clparams.StateVersion) error {
	// Read varint for length of message.
	encodedLn, _, err := ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("unable to read varint from message prefix: %v", err)
	}
	if encodedLn > uint64(16*datasize.MB) {
		return errors.New("payload too big")
	}

	sr := snappy.NewReader(r)
	raw := make([]byte, encodedLn)
	if _, err := io.ReadFull(sr, raw); err != nil {
		// fetch struct name of val
		return fmt.Errorf("unable to readPacket: %w", err)
	}

	err = val.DecodeSSZ(raw, int(version))
	if err != nil {
		return fmt.Errorf("enable to unmarshall message: %v", err)
	}
	return nil
}

func ReadUvarint(r io.Reader) (x, n uint64, err error) {
	currByte := make([]byte, 1)
	for shift := uint(0); shift < 64; shift += 7 {
		_, err := r.Read(currByte)
		n++
		if err != nil {
			return 0, 0, err
		}
		b := uint64(currByte[0])
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			// Check for overflow on the last byte
			if shift == 63 && b > 1 {
				return 0, n, errors.New("varint overflows a 64-bit integer")
			}
			return x, n, nil
		}
	}

	// The number is too large to represent in a 64-bit value.
	return 0, n, errors.New("varint overflows a 64-bit integer")
}

func DecodeListSSZ(data []byte, count uint64, list []ssz.EncodableSSZ, b *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock) error {
	objSize := list[0].EncodingSizeSSZ()

	r := bytes.NewReader(data)
	var forkDigest [4]byte

	if _, err := r.Read(forkDigest[:]); err != nil {
		return err
	}

	version, err := ethClock.StateVersionByForkDigest(forkDigest)
	if err != nil {
		return err
	}
	// Read varint for length of message.
	encodedLn, bytesCount, err := ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("failed to decode listSSZ. Unable to read varint: %v", err)
	}
	pos := 4 + bytesCount
	if len(list) != int(count) {
		return fmt.Errorf("encoded length not equal to expected size: want %d, got %d", objSize, encodedLn)
	}

	sr := snappy.NewReader(r)
	for i := 0; i < int(count); i++ {
		var n int
		raw := make([]byte, encodedLn)
		if n, err = sr.Read(raw); err != nil {
			return fmt.Errorf("readPacket: %w", err)
		}
		pos += uint64(n)

		if err := list[i].DecodeSSZ(raw, int(version)); err != nil {
			return fmt.Errorf("unmarshalling: %w", err)
		}
		r.Reset(data[pos:])
		sr.Reset(r)
	}

	return nil
}
