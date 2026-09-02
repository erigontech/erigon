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

package rlpx

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/p2p/pipes"
)

func BenchmarkHandshakeRead(b *testing.B) {
	var input = unhex(eip8HandshakeAuthTests[0].input)

	for b.Loop() {
		var (
			h   handshakeState
			r   = bytes.NewReader(input)
			msg = new(authMsgV4)
		)
		if _, err := h.readMsg(msg, keyB, r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkThroughput(b *testing.B) {
	pipe1, pipe2, err := pipes.TCPPipe()
	if err != nil {
		b.Fatal(err)
	}

	var (
		conn1, conn2  = NewConn(pipe1, nil), NewConn(pipe2, &keyA.PublicKey)
		handshakeDone = make(chan error, 1)
		msgdata       = make([]byte, 1024)
		rand          = rand.New(rand.NewSource(1337))
	)
	rand.Read(msgdata)

	// Server side.
	go func() {
		defer conn1.Close()
		// Perform handshake.
		_, err := conn1.Handshake(keyA)
		handshakeDone <- err
		if err != nil {
			return
		}
		conn1.SetSnappy(true)
		// Keep sending messages until connection closed.
		for {
			if _, err := conn1.Write(0, msgdata); err != nil {
				return
			}
		}
	}()

	// Set up client side.
	defer conn2.Close()
	if _, err := conn2.Handshake(keyB); err != nil {
		b.Fatal("client handshake error:", err)
	}
	conn2.SetSnappy(true)
	if err := <-handshakeDone; err != nil {
		b.Fatal("server hanshake error:", err)
	}

	// Read N messages.
	b.SetBytes(int64(len(msgdata)))
	b.ReportAllocs()
	for b.Loop() {
		_, _, _, err := conn2.Read()
		if err != nil {
			b.Fatal("read error:", err)
		}
	}
}
