package log

import (
	"strings"
	"testing"
)

func TestRedactArgsPreservesFlagsAndRedactsValues(t *testing.T) {
	in := []string{
		"./build/bin/erigon",
		"--chain=bor",
		"--datadir=~/erigon-data/bor-archive",
		"--log.dir.verbosity", "debug",
		"--torrent.conns.perfile", "100",
		"--torrent.maxpeers", "1000",
		"--torrent.download.slots", "10",
		"--torrent.download.rate", "1G",
		"--http.addr", "0.0.0.0",
		"--http.port", "8545",
		"--bor.heimdall", "https://polygon-heimdall-rest.publicnode.com",
		"--prune.mode=archive",
	}

	out := RedactArgs(in)

	// Executable path should be redacted to "erigon"
	if strings.Contains(out, "./build/bin/erigon") {
		t.Fatalf("expected executable path to be redacted, got: %s", out)
	}
	mustContain(t, out, "erigon")

	// Flags must be preserved
	mustContain(t, out, "--chain=bor")
	// datadir should be redacted
	if strings.Contains(out, "~/erigon-data/bor-archive") {
		t.Fatalf("expected datadir path to be redacted, got: %s", out)
	}
	mustContain(t, out, "--datadir=<redacted-dir>")
	mustContain(t, out, "--log.dir.verbosity")
	mustContain(t, out, "--torrent.conns.perfile")
	mustContain(t, out, "--torrent.maxpeers")
	mustContain(t, out, "--torrent.download.slots")
	mustContain(t, out, "--torrent.download.rate")
	mustContain(t, out, "--bor.heimdall")
	mustContain(t, out, "--prune.mode=archive")

	// Values that are not sensitive should remain
	mustContain(t, out, "debug")
	mustContain(t, out, "100")
	mustContain(t, out, "1000")
	mustContain(t, out, "10")
	mustContain(t, out, "1G")

	// Sensitive URL must be redacted
	if strings.Contains(out, "polygon-heimdall-rest.publicnode.com") {
		t.Fatalf("expected url to be redacted, got: %s", out)
	}
	mustContain(t, out, "https://<redacted>")

	// 0.0.0.0 must be redacted
	if strings.Contains(out, "0.0.0.0") {
		t.Fatalf("expected host IP to be redacted, got: %s", out)
	}
	mustContain(t, out, "--http.addr <redacted-ip>")

}

func TestRedactArgsStandaloneValues(t *testing.T) {
	in := []string{
		"cmd", "localhost:8545", "192.168.0.1:30303", "[::1]:8545", "wss://foo.bar:8443/path",
		"http://foo.com", "ws://foo.bar",
	}
	out := RedactArgs(in)
	// First arg (cmd) should be redacted to "erigon"
	mustContain(t, out, "erigon")
	if strings.Contains(out, "cmd") {
		t.Fatalf("expected executable path to be redacted, got: %s", out)
	}
	mustContain(t, out, "localhost")
	mustContain(t, out, "<redacted-ip>")
	mustContain(t, out, "<redacted-ipv6>")
	mustContain(t, out, "wss://<redacted>")
	mustContain(t, out, "http://<redacted>")
	mustContain(t, out, "ws://<redacted>")
}

func TestRedactArgsDatadir(t *testing.T) {
	// Test both --datadir= and --datadir formats
	in1 := []string{"erigon", "--datadir=/home/user/sensitive-path", "--chain=mainnet"}
	out1 := RedactArgs(in1)
	mustContain(t, out1, "--datadir=<redacted-dir>")
	if strings.Contains(out1, "/home/user/sensitive-path") {
		t.Fatalf("expected datadir path to be redacted, got: %s", out1)
	}

	in2 := []string{"erigon", "--datadir", "/home/user/another-path", "--chain=mainnet"}
	out2 := RedactArgs(in2)
	mustContain(t, out2, "--datadir <redacted-dir>")
	if strings.Contains(out2, "/home/user/another-path") {
		t.Fatalf("expected datadir path to be redacted, got: %s", out2)
	}

	// Test single dash versions
	in3 := []string{"erigon", "-datadir=/home/user/single-dash-path", "--chain=mainnet"}
	out3 := RedactArgs(in3)
	mustContain(t, out3, "-datadir=<redacted-dir>")
	if strings.Contains(out3, "/home/user/single-dash-path") {
		t.Fatalf("expected datadir path to be redacted, got: %s", out3)
	}

	in4 := []string{"erigon", "-datadir", "/home/user/another-single-dash", "--chain=mainnet"}
	out4 := RedactArgs(in4)
	mustContain(t, out4, "-datadir <redacted-dir>")
	if strings.Contains(out4, "/home/user/another-single-dash") {
		t.Fatalf("expected datadir path to be redacted, got: %s", out4)
	}
}

// helpers
func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected output to contain %q, got: %s", sub, s)
	}
}
