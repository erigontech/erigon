package sentinel

import (
	"context"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	multiplex "github.com/libp2p/go-mplex"
)

// DefaultTransport has default settings for Transport
var DefaultTransport = &Transport{}

const ID = "/mplex/6.7.0"

var _ network.Multiplexer = &Transport{}

// Transport implements mux.Multiplexer that constructs
// mplex-backed muxed connections.
type Transport struct{}

func (t *Transport) NewConn(nc net.Conn, isServer bool, scope network.PeerScope) (network.MuxedConn, error) {
	m, err := multiplex.NewMultiplex(nc, isServer, scope)
	if err != nil {
		return nil, err
	}
	return NewMuxedConn(m), nil
}

type conn multiplex.Multiplex

var _ network.MuxedConn = &conn{}

// NewMuxedConn constructs a new Conn from a *mp.Multiplex.
func NewMuxedConn(m *multiplex.Multiplex) network.MuxedConn {
	return (*conn)(m)
}

func (c *conn) Close() error {
	return c.mplex().Close()
}

func (c *conn) IsClosed() bool {
	return c.mplex().IsClosed()
}

// OpenStream creates a new stream.
func (c *conn) OpenStream(ctx context.Context) (network.MuxedStream, error) {
	s, err := c.mplex().NewStream(ctx)
	if err != nil {
		return nil, err
	}
	return (*stream)(s), nil
}

// AcceptStream accepts a stream opened by the other side.
func (c *conn) AcceptStream() (network.MuxedStream, error) {
	s, err := c.mplex().Accept()
	if err != nil {
		return nil, err
	}
	return (*stream)(s), nil
}

func (c *conn) mplex() *multiplex.Multiplex {
	return (*multiplex.Multiplex)(c)
}

// stream implements network.MuxedStream over mplex.Stream.
type stream multiplex.Stream

var _ network.MuxedStream = &stream{}

func (s *stream) Read(b []byte) (n int, err error) {
	n, err = s.mplex().Read(b)
	if err == multiplex.ErrStreamReset {
		err = network.ErrReset
	}

	return n, err
}

func (s *stream) Write(b []byte) (n int, err error) {
	n, err = s.mplex().Write(b)
	if err == multiplex.ErrStreamReset {
		err = network.ErrReset
	}

	return n, err
}

func (s *stream) Close() error {
	return s.mplex().Close()
}

func (s *stream) CloseWrite() error {
	return s.mplex().CloseWrite()
}

func (s *stream) CloseRead() error {
	return s.mplex().CloseRead()
}

func (s *stream) Reset() error {
	return s.mplex().Reset()
}

func (s *stream) SetDeadline(t time.Time) error {
	return s.mplex().SetDeadline(t)
}

func (s *stream) SetReadDeadline(t time.Time) error {
	return s.mplex().SetReadDeadline(t)
}

func (s *stream) SetWriteDeadline(t time.Time) error {
	return s.mplex().SetWriteDeadline(t)
}

func (s *stream) mplex() *multiplex.Stream {
	return (*multiplex.Stream)(s)
}
