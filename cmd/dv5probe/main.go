// dv5probe brings up a minimal discv5 endpoint, calls Resolve() against a
// target enode URL, and prints what comes back — specifically whether the
// resolved ENR carries a "chain-toml" entry.
//
// Use this to isolate whether discv5 ENR discovery is working independent
// of the rest of Erigon's startup (sentry, staging, etc.).
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/erigontech/erigon/common/crypto"
	stdlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func main() {
	target := flag.String("target", "", "enode:// URL of the peer to resolve")
	timeout := flag.Duration("timeout", 30*time.Second, "overall timeout for the probe")
	verbose := flag.Bool("v", false, "trace-level logging on the discv5 transport")
	flag.Parse()

	if *target == "" {
		fmt.Fprintln(os.Stderr, "usage: dv5probe -target enode://... [-v] [-timeout 30s]")
		os.Exit(2)
	}

	// Configure root logger so discv5 trace logs are visible when -v is set.
	lvl := stdlog.LvlInfo
	if *verbose {
		lvl = stdlog.LvlTrace
	}
	stdlog.Root().SetHandler(stdlog.LvlFilterHandler(lvl,
		stdlog.StreamHandler(os.Stderr, stdlog.TerminalFormat())))
	_ = slog.Default() // pacify import linter

	targetNode, err := enode.ParseV4(*target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse target:", err)
		os.Exit(2)
	}
	fmt.Printf("target: id=%s tcp=%v udp=%v ip=%v\n",
		targetNode.ID().TerminalString(),
		mustEndpointTCP(targetNode), mustEndpointUDP(targetNode), targetNode.IPAddr())

	// Probe identity + transient DB.
	key, err := crypto.GenerateKey()
	if err != nil {
		fmt.Fprintln(os.Stderr, "genkey:", err)
		os.Exit(1)
	}
	db, err := enode.OpenDB("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "opendb:", err)
		os.Exit(1)
	}
	defer db.Close()
	ln := enode.NewLocalNode(db, key)

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen udp:", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("probe: listening on %v, self.id=%s\n", conn.LocalAddr(), ln.ID().TerminalString())

	dv5, err := discover.ListenV5(conn, ln, discover.Config{
		PrivateKey: key,
		Log:        stdlog.Root(),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "listenv5:", err)
		os.Exit(1)
	}
	defer dv5.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)

		fmt.Println("probe: calling Ping(target)...")
		t0 := time.Now()
		if seq, err := dv5.Ping(targetNode); err != nil {
			fmt.Printf("ping FAILED in %v: %v\n", time.Since(t0), err)
		} else {
			fmt.Printf("ping ok in %v, peer ENR seq=%d\n", time.Since(t0), seq)
		}

		fmt.Println("probe: calling RequestENR(target)...")
		t0 = time.Now()
		got, err := dv5.RequestENR(targetNode)
		if err != nil {
			fmt.Printf("RequestENR FAILED in %v: %v\n", time.Since(t0), err)
		} else {
			describe("RequestENR", got)
		}

		fmt.Println("probe: calling Resolve(target)...")
		t0 = time.Now()
		got = dv5.Resolve(targetNode)
		fmt.Printf("Resolve returned in %v\n", time.Since(t0))
		describe("Resolve", got)
	}()

	select {
	case <-done:
	case <-time.After(*timeout):
		fmt.Printf("probe: timeout after %v\n", *timeout)
		os.Exit(3)
	}
}

func describe(label string, n *enode.Node) {
	if n == nil {
		fmt.Printf("%s: nil\n", label)
		return
	}
	fmt.Printf("%s: id=%s seq=%d ip=%v tcp=%v udp=%v size=%d\n",
		label, n.ID().TerminalString(), n.Seq(),
		n.IPAddr(), mustEndpointTCP(n), mustEndpointUDP(n),
		n.Record().Size())

	var ct enr.ChainToml
	if err := n.Record().Load(&ct); err != nil {
		fmt.Printf("%s: chain-toml entry: NOT PRESENT (%v)\n", label, err)
	} else {
		fmt.Printf("%s: chain-toml entry: authoritativeBlocks=%d knownBlocks=%d infoHash=%s\n",
			label, ct.AuthoritativeBlocks, ct.KnownBlocks, hex.EncodeToString(ct.InfoHash[:]))
	}
	var bt enr.BT
	if err := n.Record().Load(&bt); err != nil {
		fmt.Printf("%s: bt entry: NOT PRESENT (%v)\n", label, err)
	} else {
		fmt.Printf("%s: bt entry: port=%d\n", label, bt)
	}
}

func mustEndpointTCP(n *enode.Node) any {
	addr, ok := n.TCPEndpoint()
	if !ok {
		return "<unset>"
	}
	return addr
}
func mustEndpointUDP(n *enode.Node) any {
	addr, ok := n.UDPEndpoint()
	if !ok {
		return "<unset>"
	}
	return addr
}
