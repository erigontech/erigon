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

package enode

import (
	"context"
	"math/rand"
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enr"
)

func newLocalNodeForTesting(tmpDir string, logger log.Logger) (*LocalNode, *DB) {
	db, err := OpenDBEx(context.Background(), "", tmpDir, logger)
	if err != nil {
		panic(err)
	}
	key, _ := crypto.GenerateKey()
	return NewLocalNode(db, key), db
}

func TestLocalNode(t *testing.T) {
	tmpDir := t.TempDir()
	logger := log.New()
	ln, db := newLocalNodeForTesting(tmpDir, logger)
	defer db.Close()

	if ln.Node().ID() != ln.ID() {
		t.Fatal("inconsistent ID")
	}

	ln.Set(enr.WithEntry("x", uint(3)))
	var x uint
	if err := ln.Node().Load(enr.WithEntry("x", &x)); err != nil {
		t.Fatal("can't load entry 'x':", err)
	} else if x != 3 {
		t.Fatal("wrong value for entry 'x':", x)
	}
}

func TestLocalNodeSeqPersist(t *testing.T) {
	tmpDir := t.TempDir()
	logger := log.New()
	ln, db := newLocalNodeForTesting(tmpDir, logger)
	defer db.Close()

	initialSeq := ln.Node().Seq()
	ln.Set(enr.WithEntry("x", uint(1)))
	if s := ln.Node().Seq(); s != initialSeq+1 {
		t.Fatalf("wrong seq %d after set, want %d", s, initialSeq+1)
	}

	// Create a new instance, it should reload the sequence number.
	// The number increases just after that because a new record is
	// created without the "x" entry.
	ln2 := NewLocalNode(db, ln.key)
	if s := ln2.Node().Seq(); s != initialSeq+2 {
		t.Fatalf("wrong seq %d on new instance, want %d", s, initialSeq+2)
	}

	// Create a new instance with a different node key on the same database.
	// This should reset the sequence number, but the new initial seq is
	// time-based, so just check it's greater than zero.
	key, _ := crypto.GenerateKey()
	ln3 := NewLocalNode(db, key)
	if s := ln3.Node().Seq(); s == 0 {
		t.Fatalf("wrong seq %d on instance with changed key, want > 0", s)
	}
}

// This test checks behavior of the endpoint predictor.
func TestLocalNodeEndpoint(t *testing.T) {
	var (
		fallback  = &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 80}
		predicted = &net.UDPAddr{IP: net.IP{127, 0, 1, 2}, Port: 81}
		staticIP  = net.IP{127, 0, 1, 2}
	)
	tmpDir := t.TempDir()
	logger := log.New()
	ln, db := newLocalNodeForTesting(tmpDir, logger)
	defer db.Close()

	// Nothing is set initially.
	initialSeq := ln.Node().Seq()
	assert.Equal(t, net.IP(nil), ln.Node().IP())
	assert.Equal(t, 0, ln.Node().UDP())

	// Set up fallback address.
	ln.SetFallbackIP(fallback.IP)
	ln.SetFallbackUDP(fallback.Port)
	assert.Equal(t, fallback.IP, ln.Node().IP())
	assert.Equal(t, fallback.Port, ln.Node().UDP())
	assert.Equal(t, initialSeq+1, ln.Node().Seq())

	// Add endpoint statements from random hosts.
	for i := 0; i < iptrackMinStatements; i++ {
		assert.Equal(t, fallback.IP, ln.Node().IP())
		assert.Equal(t, fallback.Port, ln.Node().UDP())
		assert.Equal(t, initialSeq+1, ln.Node().Seq())

		fromIP := make(net.IP, 4)
		rand.Read(fromIP)
		fromAddr := netip.AddrPortFrom(netip.AddrFrom4([4]byte(fromIP)), 90)
		predictedAddr := netip.AddrPortFrom(netip.AddrFrom4([4]byte(predicted.IP.To4())), uint16(predicted.Port))
		ln.UDPEndpointStatement(fromAddr, predictedAddr)
	}
	assert.Equal(t, predicted.IP, ln.Node().IP())
	assert.Equal(t, predicted.Port, ln.Node().UDP())
	assert.Equal(t, initialSeq+2, ln.Node().Seq())

	// Static IP overrides prediction.
	ln.SetStaticIP(staticIP)
	assert.Equal(t, staticIP, ln.Node().IP())
	assert.Equal(t, fallback.Port, ln.Node().UDP())
	assert.Equal(t, initialSeq+3, ln.Node().Seq())
}
