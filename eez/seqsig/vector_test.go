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
	"encoding/json"
	"os"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/rlp"
)

// p2pVector is the signed-head test vector of eez/testdata/SOURCES.md.
type p2pVector struct {
	PrivateKey      hexutil.Bytes  `json:"privateKey"`
	Signer          common.Address `json:"signer"`
	L2ChainID       uint64         `json:"l2ChainId"`
	Registry        common.Address `json:"registry"`
	RollupID        uint64         `json:"rollupId"`
	Number          uint64         `json:"number"`
	ParentHash      common.Hash    `json:"parentHash"`
	EpochMs         uint64         `json:"epochMs"`
	Counter         uint64         `json:"counter"`
	Block           hexutil.Bytes  `json:"block"`
	BlockHash       common.Hash    `json:"blockHash"`
	DomainSeparator common.Hash    `json:"domainSeparator"`
	SigningHash     common.Hash    `json:"signingHash"`
	Signature       hexutil.Bytes  `json:"signature"`
	Envelope        hexutil.Bytes  `json:"envelope"`
}

func loadP2PVector(t *testing.T) p2pVector {
	t.Helper()
	raw, err := os.ReadFile("../testdata/vectors/signed_head_p2p_v0.json")
	require.NoError(t, err)
	var v p2pVector
	require.NoError(t, json.Unmarshal(raw, &v))
	return v
}

// TestP2PVector: the known answers of the signed-head vector, from the block
// to the envelope bytes.
func TestP2PVector(t *testing.T) {
	v := loadP2PVector(t)
	d := seqsig.Domain{ChainID: v.L2ChainID, Registry: v.Registry, RollupID: v.RollupID}
	require.Equal(t, v.DomainSeparator, d.Separator())

	b, err := seqsig.DecodeBlock(v.Block)
	require.NoError(t, err)
	require.Equal(t, v.BlockHash, b.Hash())
	reenc, err := rlp.EncodeToBytes(b)
	require.NoError(t, err)
	require.Equal(t, []byte(v.Block), reenc)

	h := seqsig.Head{RollupID: v.RollupID, Number: v.Number, BlockHash: v.BlockHash, ParentHash: v.ParentHash, EpochMs: v.EpochMs, Counter: v.Counter}
	require.Equal(t, v.SigningHash, h.SigningHash(d))
	key, err := crypto.ToECDSA(v.PrivateKey)
	require.NoError(t, err)
	require.Equal(t, v.Signer, crypto.PubkeyToAddress(key.PublicKey))
	sig, err := seqsig.Sign(h, d, key)
	require.NoError(t, err)
	require.Equal(t, []byte(v.Signature), sig)

	e := seqsig.Envelope{Head: h, Signature: sig, Block: v.Block}
	data, err := seqsig.Encode(e)
	require.NoError(t, err)
	enc, err := snappy.Decode(nil, data)
	require.NoError(t, err)
	require.Equal(t, []byte(v.Envelope), enc)

	got, err := seqsig.Decode(snappy.Encode(nil, v.Envelope))
	require.NoError(t, err)
	require.Equal(t, e, got)
	verified, err := got.Verify(d, v.Signer)
	require.NoError(t, err)
	require.Equal(t, v.BlockHash, verified.Block.Hash())
}
