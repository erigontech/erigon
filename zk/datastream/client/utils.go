package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	ErrSocket        = errors.New("socket error")
	ErrNilConnection = errors.New("nil connection")
)

func writeFullUint64ToConn(conn net.Conn, value uint64) error {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)

	if conn == nil {
		return fmt.Errorf("%w: %w", ErrSocket, ErrNilConnection)
	}

	if _, err := conn.Write(buffer); err != nil {
		return fmt.Errorf("%w: conn.Write: %v", ErrSocket, err)
	}

	return nil
}

func writeBytesToConn(conn net.Conn, value []byte) error {
	if conn == nil {
		return fmt.Errorf("%w: %w", ErrSocket, ErrNilConnection)
	}

	if _, err := conn.Write(value); err != nil {
		return fmt.Errorf("%w: conn.Write: %w", ErrSocket, err)
	}

	return nil
}

// writeFullUint32ToConn writes a uint64 to a connection
func writeFullUint32ToConn(conn net.Conn, value uint32) error {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, value)

	if conn == nil {
		return fmt.Errorf("%w: %w", ErrSocket, ErrNilConnection)
	}

	if _, err := conn.Write(buffer); err != nil {
		return fmt.Errorf("%w: conn.Write: %w", ErrSocket, err)
	}

	return nil
}

// reads a set amount of bytes from a connection
func readBuffer(conn net.Conn, n uint32) ([]byte, error) {
	buffer := make([]byte, n)
	rbc, err := io.ReadFull(conn, buffer)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: io.ReadFull: %w", ErrSocket, err)
	}

	if uint32(rbc) != n {
		return []byte{}, fmt.Errorf("expected to read %d bytes, bute got %d", n, rbc)
	}

	return buffer, nil
}
