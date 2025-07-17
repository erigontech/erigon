// Copyright 2018 The go-ethereum Authors
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

package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/erigontech/erigon/erigon-lib/log/v3"
)

// DialStdIO creates a client on stdin/stdout.
func DialStdIO(ctx context.Context, logger log.Logger) (*Client, error) {
	return DialIO(ctx, os.Stdin, os.Stdout, logger)
}

// DialIO creates a client which uses the given IO channels
func DialIO(ctx context.Context, in io.Reader, out io.Writer, logger log.Logger) (*Client, error) {
	return newClient(ctx, func(_ context.Context) (ServerCodec, error) {
		return NewCodec(stdioConn{
			in:  in,
			out: out,
		}), nil
	}, logger)
}

type stdioConn struct {
	in  io.Reader
	out io.Writer
}

func (io stdioConn) Read(b []byte) (n int, err error) {
	return io.in.Read(b)
}

func (io stdioConn) Write(b []byte) (n int, err error) {
	return io.out.Write(b)
}

func (io stdioConn) Close() error {
	return nil
}

func (io stdioConn) RemoteAddr() string {
	return "/dev/stdin"
}

func (io stdioConn) SetWriteDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "stdio", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}
