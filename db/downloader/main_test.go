package downloader

import (
	"net"
	"os"
	"testing"

	"github.com/anacrolix/envpprof"
)

func TestMain(m *testing.M) {
	// Tests in this package create BitTorrent clients that bind to local ports.
	// Skip the whole package when the environment does not permit binding.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(0)
	}
	ln.Close()
	envpprof.TestMain(m)
}
