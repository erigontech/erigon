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

package blobfetch

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetBeaconDataHidesKeyEchoedInStatusLine(t *testing.T) {
	defer func(wait time.Duration) { RetryWait = wait }(RetryWait)
	RetryWait = time.Millisecond
	const secret = "SECRET-DUMMY-KEY"
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err == nil {
				_, _ = conn.Write([]byte(strings.TrimRight(line, "\r\n") + "\r\n\r\n"))
			}
			conn.Close()
		}
	}()

	_, err = getBeaconData(t.Context(), "http://"+ln.Addr().String()+"/?apikey="+secret, "eth/v1/beacon/blobs/0x00", 1)
	require.Error(t, err)
	require.NotContains(t, err.Error(), secret)
	require.NotContains(t, err.Error(), "/eth/v1/beacon/blobs/0x00")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRedactURLErrorDropsTextAroundNetworkCause(t *testing.T) {
	const secret = "SECRET-DUMMY-KEY"
	causes := map[string]error{
		"dial":          &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")},
		"dns":           &net.DNSError{Err: "no such host", Name: "beacon.example", IsNotFound: true},
		"dial over dns": &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "beacon.example", IsNotFound: true}},
		"dns over dial": &net.DNSError{Err: "server misbehaving", Name: "beacon.example", UnwrapErr: &net.OpError{Op: "dial", Net: "udp", Err: fmt.Errorf("connection refused")}},
	}
	for name, cause := range causes {
		t.Run(name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("request %s failed: %w", req.URL, cause)
			})}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://beacon.example/eth/v1/node?apikey="+secret, nil)
			require.NoError(t, err)
			resp, err := client.Do(req)
			if resp != nil {
				resp.Body.Close()
			}
			require.Error(t, err)
			err = RedactURLError(err)
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), "/eth/v1/node")
			require.ErrorIs(t, err, cause)
			require.Equal(t, cause, errors.Unwrap(err))
		})
	}
}
