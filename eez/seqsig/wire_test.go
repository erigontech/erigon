// Copyright 2026 The Erigon Authors
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

package seqsig_test

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/seqsig"
)

// Envelope RLP of the known-answer head, by cast to-rlp of
// [[0x01, 0x07, blockHash, parentHash, 0x018bcfe56800, 0x01], signature, block].
const (
	wireNoBlockHex   = "0xf892f84c0107a00c467060700fd4695ec429a3f1eff9670ac6098f428f1552ec22ded432f3767ba0111111111111111111111111111111111111111111111111111111111111111186018bcfe5680001b8417b5b31c607467a2dd273bfd2cc34506477fa1da5f5bb8b0ec77c94855196d08e0a6bb082f1d409a8a8cb32b02f0eb4e2fdb53c12abf7a2bbfa50fa72f59493ea1c80"
	wireBlockKeccak  = "0xe133b68e84de19ba125e6a6ffb7ee01a8e9e77123e09edf43736fa3b4387cdac"
	wireBlockPayload = 0x289
)

func uncompressed(t *testing.T, data []byte) []byte {
	t.Helper()
	enc, err := snappy.Decode(nil, data)
	require.NoError(t, err)
	return enc
}

// literals returns enc as a raw Snappy block of one-byte literals, another
// valid encoding of the same payload.
func literals(enc []byte) []byte {
	out := binary.AppendUvarint(nil, uint64(len(enc)))
	for _, b := range enc {
		out = append(out, 0, b)
	}
	return out
}

func TestWireKnownAnswers(t *testing.T) {
	b := testBlock(7)
	noBlock, err := seqsig.Encode(signed(t, headOf(b), nil))
	require.NoError(t, err)
	require.Equal(t, common.FromHex(wireNoBlockHex), uncompressed(t, noBlock))

	withBlock, err := seqsig.Encode(signed(t, headOf(b), encodeBlock(t, b)))
	require.NoError(t, err)
	enc := uncompressed(t, withBlock)
	require.Equal(t, []byte{0xf9, wireBlockPayload >> 8, wireBlockPayload & 0xff}, enc[:3])
	require.Equal(t, common.HexToHash(wireBlockKeccak), crypto.Keccak256Hash(enc))
}

func TestWireRoundTrip(t *testing.T) {
	b := testBlock(7)
	for name, e := range map[string]seqsig.Envelope{
		"signed, no block":   signed(t, headOf(b), nil),
		"signed, with block": signed(t, headOf(b), encodeBlock(t, b)),
		"opaque block": {
			Head: seqsig.Head{
				RollupID: 1, Number: 9, BlockHash: common.HexToHash("0x01"),
				ParentHash: common.HexToHash("0x02"), EpochMs: 100, Counter: 3,
			},
			Signature: make([]byte, 65),
			Block:     []byte{0xc0, 1, 2, 3},
		},
	} {
		data, err := seqsig.Encode(e)
		require.NoError(t, err, name)
		got, err := seqsig.Decode(data)
		require.NoError(t, err, name)
		require.Equal(t, e, got, name)

		got, err = seqsig.Decode(literals(uncompressed(t, data)))
		require.NoError(t, err, name)
		require.Equal(t, e, got, name)
	}
}

func TestWireDecodedEnvelopeVerifies(t *testing.T) {
	b := testBlock(7)
	data, err := seqsig.Encode(signed(t, headOf(b), encodeBlock(t, b)))
	require.NoError(t, err)
	e, err := seqsig.Decode(data)
	require.NoError(t, err)
	v, err := e.Verify(domain, common.HexToAddress(signerHex))
	require.NoError(t, err)
	require.NotNil(t, v.Block)
	require.Equal(t, b.Hash(), v.Block.Hash())
}

func TestWireRejects(t *testing.T) {
	noBlock := common.FromHex(wireNoBlockHex)
	// The head list with a seventh element.
	extraField := common.FromHex("0xf893f84d0107a00c467060700fd4695ec429a3f1eff9670ac6098f428f1552ec22ded432f3767ba0111111111111111111111111111111111111111111111111111111111111111186018bcfe568000101b8417b5b31c607467a2dd273bfd2cc34506477fa1da5f5bb8b0ec77c94855196d08e0a6bb082f1d409a8a8cb32b02f0eb4e2fdb53c12abf7a2bbfa50fa72f59493ea1c80")
	// The counter 0x01 written as 0x8101.
	nonCanonical := common.FromHex("0xf893f84d0107a00c467060700fd4695ec429a3f1eff9670ac6098f428f1552ec22ded432f3767ba0111111111111111111111111111111111111111111111111111111111111111186018bcfe568008101b8417b5b31c607467a2dd273bfd2cc34506477fa1da5f5bb8b0ec77c94855196d08e0a6bb082f1d409a8a8cb32b02f0eb4e2fdb53c12abf7a2bbfa50fa72f59493ea1c80")
	// The empty block written as an empty list.
	blockList := append(append([]byte{}, noBlock[:len(noBlock)-1]...), 0xc0)

	for _, c := range []struct {
		name string
		data []byte
		want error
	}{
		{"trailing bytes", snappy.Encode(nil, append(append([]byte{}, noBlock...), 0x80)), seqsig.ErrMalformed},
		{"head with an extra field", snappy.Encode(nil, extraField), seqsig.ErrMalformed},
		{"non-canonical integer", snappy.Encode(nil, nonCanonical), seqsig.ErrMalformed},
		{"block as a list", snappy.Encode(nil, blockList), seqsig.ErrMalformed},
		{"not snappy", []byte("junk"), seqsig.ErrMalformed},
		{"empty", nil, seqsig.ErrMalformed},
		{"truncated snappy", snappy.Encode(nil, noBlock)[:20], seqsig.ErrMalformed},
		// A length header alone: rejected before the payload is allocated.
		{"declared size above the cap", binary.AppendUvarint(nil, seqsig.MaxMessageSize+1), seqsig.ErrTooLarge},
		{"decompressed size above the cap", snappy.Encode(nil, make([]byte, seqsig.MaxMessageSize+1)), seqsig.ErrTooLarge},
	} {
		_, err := seqsig.Decode(c.data)
		require.ErrorIs(t, err, c.want, c.name)
	}
	require.NoError(t, func() error { _, err := seqsig.Decode(snappy.Encode(nil, noBlock)); return err }())
}

// incompressible returns n bytes Snappy leaves at their length plus framing.
func incompressible(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{}).Read(b)
	return b
}

func TestWireEncodeCap(t *testing.T) {
	_, err := seqsig.Encode(seqsig.Envelope{Block: make([]byte, seqsig.MaxMessageSize+1)})
	require.ErrorIs(t, err, seqsig.ErrTooLarge)

	// Incompressible input whose RLP is just under the cap grows past it once
	// compressed.
	_, err = seqsig.Encode(seqsig.Envelope{Block: incompressible(seqsig.MaxMessageSize - 200)})
	require.ErrorIs(t, err, seqsig.ErrTooLarge)

	ok, err := seqsig.Encode(seqsig.Envelope{Block: make([]byte, seqsig.MaxMessageSize-200)})
	require.NoError(t, err)
	require.Less(t, len(ok), seqsig.MaxMessageSize)
}

// TestWireCapIsInclusive pins that a message of exactly the cap passes, as
// RLP on both sides of the codec and as a Snappy block on encode.
func TestWireCapIsInclusive(t *testing.T) {
	// Head list 72 bytes, signature 67, block prefix 4, list prefix 4.
	e := seqsig.Envelope{Signature: make([]byte, 65), Block: make([]byte, seqsig.MaxMessageSize-147)}
	data, err := seqsig.Encode(e)
	require.NoError(t, err)
	require.Len(t, uncompressed(t, data), seqsig.MaxMessageSize)
	got, err := seqsig.Decode(data)
	require.NoError(t, err)
	require.Equal(t, e, got)

	e.Block = append(e.Block, 0)
	_, err = seqsig.Encode(e)
	require.ErrorIs(t, err, seqsig.ErrTooLarge)
	_, err = seqsig.Decode(snappy.Encode(nil, make([]byte, seqsig.MaxMessageSize+1)))
	require.ErrorIs(t, err, seqsig.ErrTooLarge)

	// Snappy frames incompressible input at a fixed cost per 64 KiB, so
	// growing the block by the shortfall reaches a Snappy block of exactly
	// the cap.
	random := incompressible(seqsig.MaxMessageSize)
	n := seqsig.MaxMessageSize - 1024
	for range 4 {
		data, err = seqsig.Encode(seqsig.Envelope{Block: random[:n]})
		require.NoError(t, err)
		if len(data) == seqsig.MaxMessageSize {
			break
		}
		n += seqsig.MaxMessageSize - len(data)
	}
	require.Len(t, data, seqsig.MaxMessageSize)
	require.Less(t, len(uncompressed(t, data)), seqsig.MaxMessageSize)
	_, err = seqsig.Decode(data)
	require.NoError(t, err)
	_, err = seqsig.Encode(seqsig.Envelope{Block: random[:n+1]})
	require.ErrorIs(t, err, seqsig.ErrTooLarge)
}

// literalsOfLen returns enc as a raw Snappy block of exactly n bytes: its first
// bytes as one-byte literals, the rest as one literal of 2^16 to 2^24 bytes,
// whose tag takes 4 bytes.
func literalsOfLen(t *testing.T, enc []byte, n int) []byte {
	t.Helper()
	out := binary.AppendUvarint(nil, uint64(len(enc)))
	k := n - len(out) - len(enc) - 4
	rest := len(enc) - k
	require.True(t, k >= 0 && rest > 1<<16 && rest <= 1<<24, "%d one-byte literals, %d in one", k, rest)
	for _, b := range enc[:k] {
		out = append(out, 0, b)
	}
	out = append(out, 62<<2, byte(rest-1), byte((rest-1)>>8), byte((rest-1)>>16))
	out = append(out, enc[k:]...)
	require.Len(t, out, n)
	return out
}

// TestWireDecodeSnappyCap: Decode refuses a Snappy block over the cap, as
// Encode does, also when it decodes to an envelope under the cap.
func TestWireDecodeSnappyCap(t *testing.T) {
	e := seqsig.Envelope{Signature: make([]byte, 65), Block: make([]byte, seqsig.MaxMessageSize-200)}
	data, err := seqsig.Encode(e)
	require.NoError(t, err)
	enc := uncompressed(t, data)
	require.Less(t, len(enc), seqsig.MaxMessageSize)

	got, err := seqsig.Decode(literalsOfLen(t, enc, seqsig.MaxMessageSize))
	require.NoError(t, err)
	require.Equal(t, e, got)
	_, err = seqsig.Decode(literalsOfLen(t, enc, seqsig.MaxMessageSize+1))
	require.ErrorIs(t, err, seqsig.ErrTooLarge)
}
