// Copyright 2024 The Erigon Authors
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

package p2p

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/p2p/enode"
)

func PeerIdFromH512(h512 *typesproto.H512) *PeerId {
	peerId := PeerId(gointerfaces.ConvertH512ToHash(h512))
	return &peerId
}

func PeerIdFromEnode(url string) (*PeerId, error) {
	n, err := enode.ParseV4(url)
	if err != nil {
		return nil, err
	}

	b := crypto.MarshalPubkey(n.Pubkey())
	peerId := PeerId(b[:64])
	return &peerId, nil
}

// PeerIdFromUint64 is useful for testing and that is its main intended purpose
func PeerIdFromUint64(num uint64) *PeerId {
	peerId := PeerId{}
	binary.BigEndian.PutUint64(peerId[:8], num)
	return &peerId
}

type PeerId [64]byte

func (pid *PeerId) H512() *typesproto.H512 {
	return gointerfaces.ConvertHashToH512(*pid)
}

func (pid *PeerId) String() string {
	return hex.EncodeToString(pid[:])
}

func (pid *PeerId) Equal(other *PeerId) bool {
	return *pid == *other
}
