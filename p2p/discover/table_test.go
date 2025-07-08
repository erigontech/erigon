// Copyright 2015 The go-ethereum Authors
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

package discover

import (
	"crypto/ecdsa"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/netutil"
)

func TestTable_pingReplace(t *testing.T) {
	run := func(newNodeResponding, lastInBucketResponding bool) {
		name := fmt.Sprintf("newNodeResponding=%t/lastInBucketResponding=%t", newNodeResponding, lastInBucketResponding)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testPingReplace(t, newNodeResponding, lastInBucketResponding)
		})
	}

	run(true, true)
	run(false, true)
	run(true, false)
	run(false, false)
}

func testPingReplace(t *testing.T, newNodeIsResponding, lastInBucketIsResponding bool) {
	transport := newPingRecorder()
	tmpDir := t.TempDir()
	tab, db := newTestTable(transport, tmpDir, log.Root())
	defer db.Close()
	defer tab.close()

	<-tab.initDone

	// Fill up the sender's bucket.
	pingKey, _ := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	pingSender := wrapNode(enode.NewV4(&pingKey.PublicKey, net.IP{127, 0, 0, 1}, 99, 99))
	last := fillBucket(tab, pingSender)

	// Add the sender as if it just pinged us. Revalidate should replace the last node in
	// its bucket if it is unresponsive. Revalidate again to ensure that
	transport.dead[last.ID()] = !lastInBucketIsResponding
	transport.dead[pingSender.ID()] = !newNodeIsResponding
	tab.addSeenNode(pingSender)
	tab.doRevalidate(make(chan struct{}, 1))
	tab.doRevalidate(make(chan struct{}, 1))

	if !transport.pinged[last.ID()] {
		// Oldest node in bucket is pinged to see whether it is still alive.
		t.Error("table did not ping last node in bucket")
	}

	tab.mutex.Lock()
	defer tab.mutex.Unlock()
	wantSize := bucketSize
	if !lastInBucketIsResponding && !newNodeIsResponding {
		wantSize--
	}
	if l := len(tab.bucket(pingSender.ID()).entries); l != wantSize {
		t.Errorf("wrong bucket size after bond: got %d, want %d", l, wantSize)
	}
	if found := contains(tab.bucket(pingSender.ID()).entries, last.ID()); found != lastInBucketIsResponding {
		t.Errorf("last entry found: %t, want: %t", found, lastInBucketIsResponding)
	}
	wantNewEntry := newNodeIsResponding && !lastInBucketIsResponding
	if found := contains(tab.bucket(pingSender.ID()).entries, pingSender.ID()); found != wantNewEntry {
		t.Errorf("new entry found: %t, want: %t", found, wantNewEntry)
	}
}

func testTableBumpNoDuplicatesRun(t *testing.T, bucketCountGen byte, bumpCountGen byte, randGen *rand.Rand) bool {
	generateBucketNodes := func(bucketCountGen byte) []*node {
		bucketCount := bucketCountGen % (bucketSize + 1) // [0...bucketSize]
		nodes := make([]*node, bucketCount)
		for i := range nodes {
			nodes[i] = nodeAtDistance(enode.ID{}, 200, intIP(200))
		}
		return nodes
	}

	generateRandomBumpPositions := func(bumpCountGen byte, bucketCount int, randGen *rand.Rand) []int {
		bumpCount := bumpCountGen % 100
		bumps := make([]int, bumpCount)
		for i := range bumps {
			bumps[i] = randGen.Intn(bucketCount)
		}
		return bumps
	}

	nodes := generateBucketNodes(bucketCountGen)
	if len(nodes) == 0 {
		return true
	}
	bumps := generateRandomBumpPositions(bumpCountGen, len(nodes), randGen)

	if len(bumps) > 0 {
		tmpDir := t.TempDir()
		tab, db := newTestTable(newPingRecorder(), tmpDir, log.Root())
		defer db.Close()
		defer tab.close()

		b := &bucket{entries: make([]*node, len(nodes))}
		copy(b.entries, nodes)

		for i, pos := range bumps {
			tab.bumpInBucket(b, b.entries[pos])
			if hasDuplicates(b.entries) {
				t.Logf("bucket has duplicates after %d/%d bumps:", i+1, len(bumps))
				for _, n := range b.entries {
					t.Logf("  %p", n)
				}
				return false
			}
		}
		checkIPLimitInvariant(t, tab)
		return true
	}
	return true
}

func TestTable_bumpNoDuplicates_examples(t *testing.T) {
	t.Parallel()

	randGen := rand.New(rand.NewSource(time.Now().Unix()))

	t.Run("n1b1", func(t *testing.T) {
		testTableBumpNoDuplicatesRun(t, 1, 1, randGen)
	})
	t.Run("n1b5", func(t *testing.T) {
		testTableBumpNoDuplicatesRun(t, 1, 5, randGen)
	})
	t.Run("n5b1", func(t *testing.T) {
		testTableBumpNoDuplicatesRun(t, 5, 1, randGen)
	})
	t.Run("n5b10", func(t *testing.T) {
		testTableBumpNoDuplicatesRun(t, 5, 10, randGen)
	})
	t.Run("n16b10", func(t *testing.T) {
		testTableBumpNoDuplicatesRun(t, 16, 10, randGen)
	})
	t.Run("n16b90", func(t *testing.T) {
		testTableBumpNoDuplicatesRun(t, 16, 90, randGen)
	})
}

// This checks that the table-wide IP limit is applied correctly.
func TestTable_IPLimit(t *testing.T) {
	transport := newPingRecorder()
	tmpDir := t.TempDir()
	tab, db := newTestTable(transport, tmpDir, log.Root())
	defer db.Close()
	defer tab.close()

	for i := 0; i < tableIPLimit+1; i++ {
		n := nodeAtDistance(tab.self().ID(), i, net.IP{172, 0, 1, byte(i)})
		tab.addSeenNode(n)
	}
	if tab.len() > tableIPLimit {
		t.Errorf("too many nodes in table")
	}
	checkIPLimitInvariant(t, tab)
}

// This checks that the per-bucket IP limit is applied correctly.
func TestTable_BucketIPLimit(t *testing.T) {
	transport := newPingRecorder()
	tmpDir := t.TempDir()
	tab, db := newTestTable(transport, tmpDir, log.Root())
	defer db.Close()
	defer tab.close()

	d := 3
	for i := 0; i < bucketIPLimit+1; i++ {
		n := nodeAtDistance(tab.self().ID(), d, net.IP{172, 0, 1, byte(i)})
		tab.addSeenNode(n)
	}
	if tab.len() > bucketIPLimit {
		t.Errorf("too many nodes in table")
	}
	checkIPLimitInvariant(t, tab)
}

// checkIPLimitInvariant checks that ip limit sets contain an entry for every
// node in the table and no extra entries.
func checkIPLimitInvariant(t *testing.T, tab *Table) {
	t.Helper()

	tabset := netutil.DistinctNetSet{Subnet: tableSubnet, Limit: tableIPLimit}
	for _, b := range tab.buckets {
		for _, n := range b.entries {
			tabset.Add(n.IP())
		}
	}
	if tabset.String() != tab.ips.String() {
		t.Errorf("table IP set is incorrect:\nhave: %v\nwant: %v", tab.ips, tabset)
	}
}

func testTableFindNodeByIDRun(t *testing.T, nodesCountGen uint16, resultsCountGen byte, rand *rand.Rand) bool {
	if !t.Skipped() {
		// for any node table, Target and N
		transport := newPingRecorder()
		tmpDir := t.TempDir()
		tab, db := newTestTable(transport, tmpDir, log.Root())
		defer db.Close()
		defer tab.close()

		nodesCount := int(nodesCountGen) % (bucketSize*nBuckets + 1)
		testNodes := generateNodes(rand, nodesCount)
		fillTable(tab, testNodes)

		target := enode.ID{}
		resultsCount := int(resultsCountGen) % (bucketSize + 1)

		// check that closest(Target, N) returns nodes
		result := tab.findnodeByID(target, resultsCount, false).entries
		if hasDuplicates(result) {
			t.Errorf("result contains duplicates")
			return false
		}
		if !sortedByDistanceTo(target, result) {
			t.Errorf("result is not sorted by distance to target")
			return false
		}

		// check that the number of results is min(N, tablen)
		wantN := resultsCount
		if tlen := tab.len(); tlen < resultsCount {
			wantN = tlen
		}
		if len(result) != wantN {
			t.Errorf("wrong number of nodes: got %d, want %d", len(result), wantN)
			return false
		} else if len(result) == 0 {
			return true // no need to check distance
		}

		// check that the result nodes have minimum distance to target.
		for _, b := range tab.buckets {
			for _, n := range b.entries {
				if contains(result, n.ID()) {
					continue // don't run the check below for nodes in result
				}
				farthestResult := result[len(result)-1].ID()
				if enode.DistCmp(target, n.ID(), farthestResult) < 0 {
					t.Errorf("table contains node that is closer to target but it's not in result")
					t.Logf("  Target:          %v", target)
					t.Logf("  Farthest Result: %v", farthestResult)
					t.Logf("  ID:              %v", n.ID())
					return false
				}
			}
		}
		return true
	}
	return true
}

func TestTable_findNodeByID_examples(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	randGen := rand.New(rand.NewSource(time.Now().Unix()))

	t.Run("n0r1", func(t *testing.T) {
		testTableFindNodeByIDRun(t, 0, 1, randGen)
	})
	t.Run("n1r1", func(t *testing.T) {
		testTableFindNodeByIDRun(t, 1, 1, randGen)
	})
	t.Run("n16r1", func(t *testing.T) {
		testTableFindNodeByIDRun(t, bucketSize, 1, randGen)
	})
	t.Run("nMr1", func(t *testing.T) {
		testTableFindNodeByIDRun(t, uint16(bucketSize*nBuckets), 1, randGen)
	})
	t.Run("n0r2", func(t *testing.T) {
		testTableFindNodeByIDRun(t, 0, 2, randGen)
	})
	t.Run("n1r2", func(t *testing.T) {
		testTableFindNodeByIDRun(t, 1, 2, randGen)
	})
	t.Run("n16r2", func(t *testing.T) {
		testTableFindNodeByIDRun(t, bucketSize, 2, randGen)
	})
	t.Run("nMr2", func(t *testing.T) {
		testTableFindNodeByIDRun(t, uint16(bucketSize*nBuckets), 2, randGen)
	})
	t.Run("n0rM", func(t *testing.T) {
		testTableFindNodeByIDRun(t, 0, bucketSize, randGen)
	})
	t.Run("n1rM", func(t *testing.T) {
		testTableFindNodeByIDRun(t, 1, bucketSize, randGen)
	})
	t.Run("n16rM", func(t *testing.T) {
		testTableFindNodeByIDRun(t, bucketSize, bucketSize, randGen)
	})
	t.Run("nMrM", func(t *testing.T) {
		testTableFindNodeByIDRun(t, uint16(bucketSize*nBuckets), bucketSize, randGen)
	})
}

func testTableReadRandomNodesGetAllRun(t *testing.T, nodesCountGen uint16, rand *rand.Rand) bool {
	nodesCount := nodesCountGen % 1000
	if nodesCount > 0 {
		buf := make([]*enode.Node, nodesCount)
		transport := newPingRecorder()
		tmpDir := t.TempDir()
		tab, db := newTestTable(transport, tmpDir, log.Root())
		defer db.Close()
		defer tab.close()
		<-tab.initDone

		for i := 0; i < len(buf); i++ {
			ld := rand.Intn(len(tab.buckets))
			fillTable(tab, []*node{nodeAtDistance(tab.self().ID(), ld, intIP(ld))})
		}
		gotN := tab.ReadRandomNodes(buf)
		if gotN != tab.len() {
			t.Errorf("wrong number of nodes, got %d, want %d", gotN, tab.len())
			return false
		}
		if hasDuplicates(wrapNodes(buf[:gotN])) {
			t.Errorf("result contains duplicates")
			return false
		}
		return true
	}
	return true
}

func TestTable_ReadRandomNodesGetAll_examples(t *testing.T) {
	t.Parallel()

	randGen := rand.New(rand.NewSource(time.Now().Unix()))

	t.Run("n1", func(t *testing.T) {
		testTableReadRandomNodesGetAllRun(t, 1, randGen)
	})
	t.Run("n2", func(t *testing.T) {
		testTableReadRandomNodesGetAllRun(t, 2, randGen)
	})
	t.Run("n20", func(t *testing.T) {
		testTableReadRandomNodesGetAllRun(t, 20, randGen)
	})
	t.Run("n200", func(t *testing.T) {
		testTableReadRandomNodesGetAllRun(t, 200, randGen)
	})
}

func generateNodes(rand *rand.Rand, count int) []*node {
	nodes := make([]*node, 0, count)
	for i := 0; i < count; i++ {
		nodes = append(nodes, generateNode(rand))
	}
	return nodes
}

func generateNode(rand *rand.Rand) *node {
	var id enode.ID
	rand.Read(id[:])

	r := new(enr.Record)
	r.Set(enr.IP(genIP(rand)))

	n := wrapNode(enode.SignNull(r, id))
	n.livenessChecks = 1
	return n
}

func TestTable_addVerifiedNode(t *testing.T) {
	tmpDir := t.TempDir()
	tab, db := newTestTable(newPingRecorder(), tmpDir, log.Root())
	<-tab.initDone
	defer db.Close()
	defer tab.close()

	// Insert two nodes.
	n1 := nodeAtDistance(tab.self().ID(), 256, net.IP{88, 77, 66, 1})
	n2 := nodeAtDistance(tab.self().ID(), 256, net.IP{88, 77, 66, 2})
	tab.addSeenNode(n1)
	tab.addSeenNode(n2)

	// Verify bucket content:
	bcontent := []*node{n1, n2}
	if !reflect.DeepEqual(tab.bucket(n1.ID()).entries, bcontent) {
		t.Fatalf("wrong bucket content: %v", tab.bucket(n1.ID()).entries)
	}

	// Add a changed version of n2.
	newrec := n2.Record()
	newrec.Set(enr.IP{99, 99, 99, 99})
	newn2 := wrapNode(enode.SignNull(newrec, n2.ID()))
	tab.addVerifiedNode(newn2)

	// Check that bucket is updated correctly.
	newBcontent := []*node{newn2, n1}
	if !reflect.DeepEqual(tab.bucket(n1.ID()).entries, newBcontent) {
		t.Fatalf("wrong bucket content after update: %v", tab.bucket(n1.ID()).entries)
	}
	checkIPLimitInvariant(t, tab)
}

func TestTable_addSeenNode(t *testing.T) {
	tmpDir := t.TempDir()
	tab, db := newTestTable(newPingRecorder(), tmpDir, log.Root())
	<-tab.initDone
	defer db.Close()
	defer tab.close()

	// Insert two nodes.
	n1 := nodeAtDistance(tab.self().ID(), 256, net.IP{88, 77, 66, 1})
	n2 := nodeAtDistance(tab.self().ID(), 256, net.IP{88, 77, 66, 2})
	tab.addSeenNode(n1)
	tab.addSeenNode(n2)

	// Verify bucket content:
	bcontent := []*node{n1, n2}
	if !reflect.DeepEqual(tab.bucket(n1.ID()).entries, bcontent) {
		t.Fatalf("wrong bucket content: %v", tab.bucket(n1.ID()).entries)
	}

	// Add a changed version of n2.
	newrec := n2.Record()
	newrec.Set(enr.IP{99, 99, 99, 99})
	newn2 := wrapNode(enode.SignNull(newrec, n2.ID()))
	tab.addSeenNode(newn2)

	// Check that bucket content is unchanged.
	if !reflect.DeepEqual(tab.bucket(n1.ID()).entries, bcontent) {
		t.Fatalf("wrong bucket content after update: %v", tab.bucket(n1.ID()).entries)
	}
	checkIPLimitInvariant(t, tab)
}

// This test checks that ENR updates happen during revalidation. If a node in the table
// announces a new sequence number, the new record should be pulled.
func TestTable_revalidateSyncRecord(t *testing.T) {
	transport := newPingRecorder()
	tmpDir := t.TempDir()
	tab, db := newTestTable(transport, tmpDir, log.Root())
	<-tab.initDone
	defer db.Close()
	defer tab.close()

	// Insert a node.
	var r enr.Record
	r.Set(enr.IP(net.IP{127, 0, 0, 1}))
	id := enode.ID{1}
	n1 := wrapNode(enode.SignNull(&r, id))
	tab.addSeenNode(n1)

	// Update the node record.
	r.Set(enr.WithEntry("foo", "bar"))
	n2 := enode.SignNull(&r, id)
	transport.updateRecord(n2)

	tab.doRevalidate(make(chan struct{}, 1))
	intable := tab.getNode(id)
	if !reflect.DeepEqual(intable, n2) {
		t.Fatalf("table contains old record with seq %d, want seq %d", intable.Seq(), n2.Seq())
	}
}

func genIP(rand *rand.Rand) net.IP {
	ip := make(net.IP, 4)
	rand.Read(ip)
	return ip
}

func newkey() *ecdsa.PrivateKey {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("couldn't generate key: " + err.Error())
	}
	return key
}
