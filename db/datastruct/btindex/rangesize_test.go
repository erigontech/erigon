//go:build linux

package btindex

import (
	"os"
	"strings"
	"testing"

	"github.com/erigontech/erigon/db/seg"
)

// TestLeafRangeSize reports how big the bt-node-derived offset range is (the
// window bs() returns): records per range (nodeStride) and bytes per range.
func TestLeafRangeSize(t *testing.T) {
	kvPath := os.Getenv("KV_FILE")
	if kvPath == "" {
		kvPath = "/erigon-data/jochemnet36_merged/snapshots/domain/v2.2-commitment.1049728-1049792.kv"
	}
	if _, err := os.Stat(kvPath); err != nil {
		t.Skipf("no file: %v", err)
	}
	btPath := strings.Replace(strings.TrimSuffix(kvPath, ".kv")+".bt", "v2.2-", "v2.0-", 1)

	d, err := seg.NewDecompressor(kvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	rd := seg.NewReader(d.MakeGetter(), seg.CompressKeys) // commitment: keys compressed
	bt, err := OpenBtreeIndexWithDecompressor(btPath, DefaultBtreeM, rd)
	if err != nil {
		t.Fatal(err)
	}
	defer bt.Close()

	bp := bt.bplus
	N := bp.offt.Count()
	nodes := uint64(bp.numNodes())
	if nodes == 0 || N == 0 {
		t.Skip("empty")
	}
	dataBytes := bp.offt.Get(N-1) - bp.offt.Get(0)
	t.Logf("file=%s", kvPath)
	t.Logf("records N=%d  pivots=%d  nodeStride=%d records/range", N, nodes, bp.nodeStride)
	t.Logf("data bytes=%.2f GB  avg record=%d B  avg RANGE=%d B (%.1f KB) = %d records",
		float64(dataBytes)/1e9, dataBytes/N,
		dataBytes/nodes, float64(dataBytes)/float64(nodes)/1024, N/nodes)

	// Sample actual bs() ranges for a spread of pivot keys.
	var sumRecs, sumBytes, cnt uint64
	var maxBytes uint64
	for i := uint64(1); i < nodes-1; i += max(1, (nodes-2)/2000) {
		k := bp.nodeKey(int(i))
		dl, dr, _, _ := bp.bs(k)
		if dr <= dl || dr > N {
			continue
		}
		recs := dr - dl
		b := bp.offt.Get(dr) - bp.offt.Get(dl)
		sumRecs += recs
		sumBytes += b
		if b > maxBytes {
			maxBytes = b
		}
		cnt++
	}
	if cnt > 0 {
		t.Logf("sampled %d bs() ranges: mean=%d records / %d B (%.1f KB), max=%d B (%.1f KB)",
			cnt, sumRecs/cnt, sumBytes/cnt, float64(sumBytes)/float64(cnt)/1024, maxBytes, float64(maxBytes)/1024)
	}
}
