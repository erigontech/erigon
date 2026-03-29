package jsonrpc

import (
	"net"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Tests in this package require network port binding.
	// Skip the whole package when the environment does not permit it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(0)
	}
	ln.Close()
	os.Exit(m.Run())
}
