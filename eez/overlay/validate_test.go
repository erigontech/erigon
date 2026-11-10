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

package overlay

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
	"github.com/holiman/uint256"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

var testDomain = seqsig.Domain{
	ChainID:  1337,
	Registry: common.BytesToAddress(bytes.Repeat([]byte{0xaa}, 20)),
	RollupID: 1,
}

// key returns test key i, the secp256k1 scalar i+1.
func key(i byte) *ecdsa.PrivateKey {
	var b [32]byte
	b[31] = i + 1
	k, err := crypto.ToECDSA(b[:])
	if err != nil {
		panic(err)
	}
	return k
}

func addr(i byte) common.Address { return crypto.PubkeyToAddress(key(i).PublicKey) }

func head(epochMs, counter uint64, hash byte) seqsig.Head {
	return seqsig.Head{
		RollupID: testDomain.RollupID, Number: counter, BlockHash: common.Hash{hash},
		ParentHash: common.Hash{0x11}, EpochMs: epochMs, Counter: counter,
	}
}

func signed(t *testing.T, signer byte, h seqsig.Head, block []byte) seqsig.Envelope {
	t.Helper()
	sig, err := seqsig.Sign(h, testDomain, key(signer))
	require.NoError(t, err)
	return seqsig.Envelope{Head: h, Signature: sig, Block: block}
}

func encode(t *testing.T, e seqsig.Envelope) []byte {
	t.Helper()
	data, err := seqsig.Encode(e)
	require.NoError(t, err)
	return data
}

// mustEncode returns the wire encoding of h signed by key signer.
func mustEncode(signer byte, h seqsig.Head) []byte {
	sig, err := seqsig.Sign(h, testDomain, key(signer))
	if err != nil {
		panic(err)
	}
	data, err := seqsig.Encode(seqsig.Envelope{Head: h, Signature: sig})
	if err != nil {
		panic(err)
	}
	return data
}

// reencode returns data's payload as another Snappy block: literals of at
// most run bytes, run at most 60.
func reencode(data []byte, run int) []byte {
	raw, err := snappy.Decode(nil, data)
	if err != nil {
		panic(err)
	}
	out := binary.AppendUvarint(nil, uint64(len(raw)))
	for len(raw) > 0 {
		n := min(run, len(raw))
		out = append(out, byte(n-1)<<2)
		out = append(out, raw[:n]...)
		raw = raw[n:]
	}
	return out
}

func TestRelayRule(t *testing.T) {
	const now = 1_000_000
	v := &validator{domain: testDomain, signer: addr(0), now: func() uint64 { return now }}
	first := encode(t, signed(t, 0, head(100, 1, 1), nil))

	tampered := signed(t, 0, head(100, 9, 1), nil)
	tampered.Head.Number++
	otherRollup := head(100, 9, 1)
	otherRollup.RollupID = 2
	blockEnc, err := rlp.EncodeToBytes(types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(2)}, nil))
	require.NoError(t, err)
	trailing, err := snappy.Decode(nil, first)
	require.NoError(t, err)
	oversize := binary.AppendUvarint(nil, seqsig.MaxMessageSize+1)

	steps := []struct {
		name string
		data []byte
		want pubsub.ValidationResult
	}{
		{"undecodable", []byte("junk"), pubsub.ValidationReject},
		{"above the size cap", oversize, pubsub.ValidationReject},
		{"trailing bytes", snappy.Encode(nil, append(trailing, 0x80)), pubsub.ValidationReject},
		{"first head", first, pubsub.ValidationAccept},
		{"same message", first, pubsub.ValidationIgnore},
		{"re-encoded copy", reencode(first, 1), pubsub.ValidationIgnore},
		{"replay below the latest", encode(t, signed(t, 0, head(100, 0, 1), nil)), pubsub.ValidationIgnore},
		{"epoch ahead of local time", encode(t, signed(t, 0, head(now+seqsig.MaxFutureMs+1, 1, 1), nil)), pubsub.ValidationIgnore},
		{"unauthorized signer", encode(t, signed(t, 1, head(100, 2, 1), nil)), pubsub.ValidationReject},
		{"other rollup", encode(t, signed(t, 0, otherRollup, nil)), pubsub.ValidationReject},
		{"tampered head", encode(t, tampered), pubsub.ValidationReject},
		{"block not the head's", encode(t, signed(t, 0, head(100, 2, 1), blockEnc)), pubsub.ValidationReject},
		{"equivocation", encode(t, signed(t, 0, head(100, 1, 2), nil)), pubsub.ValidationReject},
		{"next head", encode(t, signed(t, 0, head(100, 2, 2), nil)), pubsub.ValidationAccept},
		{"restarted signer", encode(t, signed(t, 0, head(200, 1, 3), nil)), pubsub.ValidationAccept},
	}
	for _, s := range steps {
		h, got, err := v.check(s.data)
		require.Equal(t, s.want, got, "%s: %v", s.name, err)
		require.Equal(t, s.want == pubsub.ValidationAccept, h != nil, s.name)
		require.Equal(t, s.want == pubsub.ValidationReject, err != nil, s.name)
	}

	// Past MaxSources, a new source with the lowest epoch is stale at once.
	for i := range uint64(seqsig.MaxSources) {
		_, got, _ := v.check(encode(t, signed(t, 0, head(300+i, 1, 4), nil)))
		require.Equal(t, pubsub.ValidationAccept, got, "source %d", i)
	}
	_, got, err := v.check(encode(t, signed(t, 0, head(50, 1, 5), nil)))
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationIgnore, got, "evicted source")
}

func TestMessageID(t *testing.T) {
	data := encode(t, signed(t, 0, head(100, 1, 1), nil))
	want := sha256.Sum256(data)
	require.Equal(t, string(want[:]), msgID(&pubsubpb.Message{Data: data}))
}
