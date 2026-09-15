// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package v5wire

import (
	"net/netip"
	"testing"
)

// This benchmark checks performance of handshake packet decoding.
func BenchmarkV5_DecodeHandshakePingSecp256k1(b *testing.B) {
	net := newHandshakeTest()
	defer net.close()

	var (
		idA       = net.nodeA.id()
		challenge = &Whoareyou{Node: net.nodeB.n()}
		message   = &Ping{ReqID: []byte("reqid")}
	)
	enc, _, err := net.nodeA.c.Encode(net.nodeB.id(), netip.AddrPort{}, message, challenge)
	if err != nil {
		b.Fatal("can't encode handshake packet")
	}
	challenge.Node = nil // force ENR signature verification in decoder

	input := make([]byte, len(enc))
	for b.Loop() {
		copy(input, enc)
		net.nodeB.c.sc.storeSentHandshake(idA, netip.AddrPort{}, challenge)
		_, _, _, err := net.nodeB.c.Decode(input, netip.AddrPort{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// This benchmark checks how long it takes to decode an encrypted ping packet.
func BenchmarkV5_DecodePing(b *testing.B) {
	net := newHandshakeTest()
	defer net.close()

	session := &session{
		readKey:  []byte{233, 203, 93, 195, 86, 47, 177, 186, 227, 43, 2, 141, 244, 230, 120, 17},
		writeKey: []byte{79, 145, 252, 171, 167, 216, 252, 161, 208, 190, 176, 106, 214, 39, 178, 134},
	}
	net.nodeA.c.sc.storeNewSession(net.nodeB.id(), net.nodeB.addr(), session, net.nodeB.n())
	net.nodeB.c.sc.storeNewSession(net.nodeA.id(), net.nodeA.addr(), session.keysFlipped(), net.nodeA.n())
	addrB := net.nodeA.addr()
	ping := &Ping{ReqID: []byte("reqid"), ENRSeq: 5}
	enc, _, err := net.nodeA.c.Encode(net.nodeB.id(), addrB, ping, nil)
	if err != nil {
		b.Fatalf("can't encode: %v", err)
	}

	input := make([]byte, len(enc))
	for b.Loop() {
		copy(input, enc)
		_, _, packet, _ := net.nodeB.c.Decode(input, addrB)
		if _, ok := packet.(*Ping); !ok {
			b.Fatalf("wrong packet type %T", packet)
		}
	}
}
