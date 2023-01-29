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

package ssz_snappy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/c2h5oh/datasize"
	"github.com/golang/snappy"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/fork"
)

func EncodeAndWrite(w io.Writer, val ssz_utils.Marshaler, prefix ...byte) error {
	// create prefix for length of packet
	lengthBuf := make([]byte, 10)
	vin := binary.PutUvarint(lengthBuf, uint64(val.EncodingSizeSSZ()))
	// Create writer size
	wr := bufio.NewWriterSize(w, 10+val.EncodingSizeSSZ())
	defer wr.Flush()
	// Write length of packet
	wr.Write(prefix)
	wr.Write(lengthBuf[:vin])
	// start using streamed snappy compression
	sw := snappy.NewBufferedWriter(wr)
	defer sw.Flush()
	// Marshall and snap it
	enc := make([]byte, 0, val.EncodingSizeSSZ())
	var err error
	enc, err = val.EncodeSSZ(enc)
	if err != nil {
		return err
	}
	_, err = sw.Write(enc)
	return err
}

func DecodeAndRead(r io.Reader, val ssz_utils.EncodableSSZ, b *clparams.BeaconChainConfig, genesisValidatorRoot libcommon.Hash) error {
	var forkDigest [4]byte
	// TODO(issues/5884): assert the fork digest matches the expectation for
	// a specific configuration.
	if _, err := r.Read(forkDigest[:]); err != nil {
		return err
	}
	version, err := fork.ForkDigestVersion(forkDigest, b, genesisValidatorRoot)
	if err != nil {
		return err
	}
	return DecodeAndReadNoForkDigest(r, val, version)
}

func DecodeAndReadNoForkDigest(r io.Reader, val ssz_utils.EncodableSSZ, version clparams.StateVersion) error {
	// Read varint for length of message.
	encodedLn, _, err := ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("unable to read varint from message prefix: %v", err)
	}
	if encodedLn > uint64(16*datasize.MB) {
		return fmt.Errorf("payload too big")
	}

	sr := snappy.NewReader(r)
	raw := make([]byte, encodedLn)
	if _, err := io.ReadFull(sr, raw); err != nil {
		return fmt.Errorf("unable to readPacket: %w", err)
	}

	err = val.DecodeSSZWithVersion(raw, int(version))
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
			return x, n, nil
		}
	}

	// The number is too large to represent in a 64-bit value.
	return 0, n, nil
}

func DecodeListSSZ(data []byte, count uint64, list []ssz_utils.EncodableSSZ, b *clparams.BeaconChainConfig, genesisValidatorRoot libcommon.Hash) error {
	objSize := list[0].EncodingSizeSSZ()

	r := bytes.NewReader(data)
	var forkDigest [4]byte
	// TODO(issues/5884): assert the fork digest matches the expectation for
	// a specific configuration.
	if _, err := r.Read(forkDigest[:]); err != nil {
		return err
	}

	version, err := fork.ForkDigestVersion(forkDigest, b, genesisValidatorRoot)
	if err != nil {
		return err
	}
	// Read varint for length of message.
	encodedLn, bytesCount, err := ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("unable to read varint from message prefix: %v", err)
	}
	pos := 4 + bytesCount
	if encodedLn != uint64(objSize) || len(list) != int(count) {
		return fmt.Errorf("encoded length not equal to expected size: want %d, got %d", objSize, encodedLn)
	}

	sr := snappy.NewReader(r)
	for i := 0; i < int(count); i++ {
		var n int
		raw := make([]byte, objSize)
		if n, err = sr.Read(raw); err != nil {
			return fmt.Errorf("readPacket: %w", err)
		}
		pos += uint64(n)

		if err := list[i].DecodeSSZWithVersion(raw, int(version)); err != nil {
			return fmt.Errorf("unmarshalling: %w", err)
		}
		r.Reset(data[pos:])
		sr.Reset(r)
	}

	return nil
}
