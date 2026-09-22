package commitment

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/stretchr/testify/require"
)

type recordShapeCtx struct {
	inner      PatriciaContext
	collecting bool
	records    int
	totalBytes int
	cells      int
	ext        int
	accAddr    int
	storAddr   int
	hash       int
	stateHash  int
	nAcc       int
	nStor      int
	nHash      int
	nStateHash int
}

func (c *recordShapeCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	v, s, err := c.inner.Branch(prefix)
	if c.collecting && len(v) > 2 {
		c.records++
		c.totalBytes += len(v)
		var row [16]cell
		if _, derr := DecodeBranchInto(v[2:], false, &row); derr == nil {
			for i := range row {
				cl := &row[i]
				if cl.extLen == 0 && cl.accountAddrLen == 0 && cl.storageAddrLen == 0 &&
					cl.hashLen == 0 && cl.stateHashLen == 0 {
					continue
				}
				c.cells++
				c.ext += int(cl.extLen)
				c.accAddr += int(cl.accountAddrLen)
				c.storAddr += int(cl.storageAddrLen)
				c.hash += int(cl.hashLen)
				c.stateHash += int(cl.stateHashLen)
				if cl.accountAddrLen > 0 {
					c.nAcc++
				}
				if cl.storageAddrLen > 0 {
					c.nStor++
				}
				if cl.hashLen > 0 {
					c.nHash++
				}
				if cl.stateHashLen > 0 {
					c.nStateHash++
				}
			}
		}
	}
	return v, s, err
}

func (c *recordShapeCtx) PutBranch(p, d, pr []byte) error   { return c.inner.PutBranch(p, d, pr) }
func (c *recordShapeCtx) Account(k []byte) (*Update, error) { return c.inner.Account(k) }
func (c *recordShapeCtx) Storage(k []byte) (*Update, error) { return c.inner.Storage(k) }

func TestBranchRecordComposition(t *testing.T) {
	pk, upds := buildWhaleCorpus(whale1M())
	dk, du := buildDelta(pk, upds, 500, 4242)
	ctx := context.Background()

	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	cc := &recordShapeCtx{inner: ms}
	hph := NewHexPatriciaHashed(length.Addr, cc, DefaultTrieConfig())
	u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	_, err := hph.Process(ctx, u1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u1.Close()

	require.NoError(t, ms.applyPlainUpdates(dk, du))
	cc.collecting = true
	u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, dk, du)
	_, err = hph.Process(ctx, u2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2.Close()

	pct := func(n int) float64 { return 100 * float64(n) / float64(cc.totalBytes) }
	t.Logf("records=%d totalBytes=%d avgRecord=%.0f cells=%d avgCellsPerRecord=%.1f",
		cc.records, cc.totalBytes, float64(cc.totalBytes)/float64(cc.records),
		cc.cells, float64(cc.cells)/float64(cc.records))
	t.Logf("  hash       %8d B (%4.1f%%) over %d cells", cc.hash, pct(cc.hash), cc.nHash)
	t.Logf("  stateHash  %8d B (%4.1f%%) over %d cells", cc.stateHash, pct(cc.stateHash), cc.nStateHash)
	t.Logf("  accountAddr%8d B (%4.1f%%) over %d cells", cc.accAddr, pct(cc.accAddr), cc.nAcc)
	t.Logf("  storageAddr%8d B (%4.1f%%) over %d cells", cc.storAddr, pct(cc.storAddr), cc.nStor)
	t.Logf("  extension  %8d B (%4.1f%%)", cc.ext, pct(cc.ext))
	keys := cc.accAddr + cc.storAddr
	t.Logf("  => plain-key refs = %d B (%.1f%%); hashes = %d B (%.1f%%)",
		keys, pct(keys), cc.hash+cc.stateHash, pct(cc.hash+cc.stateHash))
}
