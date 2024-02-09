package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	ErrBadBookmark = errors.New("bad bookmark")
)

// writeFullUint64ToConn writes a uint64 to a connection
func writeFullUint64ToConn(conn net.Conn, value uint64) error {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)

	if conn == nil {
		return errors.New("error nil connection")
	}

	_, err := conn.Write(buffer)
	if err != nil {
		return fmt.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
	}

	return nil
}

// writeFullUint64ToConn writes a uint64 to a connection
func writeBytesToConn(conn net.Conn, value []byte) error {
	if conn == nil {
		return errors.New("error nil connection")
	}

	_, err := conn.Write(value)
	if err != nil {
		return fmt.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
	}

	return nil
}

// writeFullUint32ToConn writes a uint64 to a connection
func writeFullUint32ToConn(conn net.Conn, value uint32) error {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, value)

	if conn == nil {
		return errors.New("error nil connection")
	}

	_, err := conn.Write(buffer)
	if err != nil {
		return fmt.Errorf("%s Error sending to server: %v", conn.RemoteAddr().String(), err)
	}

	return nil
}

// reads a set amount of bytes from a connection
func readBuffer(conn net.Conn, n uint32) ([]byte, error) {
	buffer := make([]byte, n)
	rbc, err := io.ReadFull(conn, buffer)
	if err != nil {
		return []byte{}, parseIoReadError(err)
	}

	if uint32(rbc) != n {
		return []byte{}, fmt.Errorf("expected to read %d bytes, bute got %d", n, rbc)
	}

	return buffer, nil
}

// parseIoReadError parses an error returned from io.ReadFull and returns a more concrete one
func parseIoReadError(err error) error {
	if err == io.EOF {
		return errors.New("server close connection")
	} else {
		return fmt.Errorf("error reading from server: %v", err)
	}
}
