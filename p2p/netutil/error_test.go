// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package netutil

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This test checks that isPacketTooBig correctly identifies
// errors that result from receiving a UDP packet larger
// than the supplied receive buffer.
func TestIsPacketTooBig(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := listener.Close()
		require.NoError(t, err)
	})
	sender, err := net.Dial("udp", listener.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := sender.Close()
		require.NoError(t, err)
	})

	sendN := 1800
	recvN := 300
	for i := 0; i < 20; i++ {
		go func() {
			buf := make([]byte, sendN)
			for i := range buf {
				buf[i] = byte(i)
			}
			_, err := sender.Write(buf)
			require.NoError(t, err)
		}()

		buf := make([]byte, recvN)
		err := listener.SetDeadline(time.Now().Add(1 * time.Second))
		require.NoError(t, err)
		n, _, err := listener.ReadFrom(buf)
		if err != nil {
			var nerr net.Error
			if errors.As(err, &nerr) && nerr.Timeout() {
				continue
			}
			if !isPacketTooBig(err) {
				t.Fatalf("unexpected read error: %v", err)
			}
			continue
		}
		if n != recvN {
			t.Fatalf("short read: %d, want %d", n, recvN)
		}
		for i := range buf {
			if buf[i] != byte(i) {
				t.Fatalf("error in pattern")
			}
		}
	}
}
