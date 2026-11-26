package state

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	coldSloadTxs      = 64
	coldSloadPerTx    = 128
	coldSloadHotPerTx = 32
)

type coldSloadBlock struct {
	ibs   *IntraBlockState
	addr  accounts.Address
	slots []accounts.StorageKey
	// touched gives the contract a version-map cell in a slot nobody reads.
	touched bool
}

// newColdSloadBlock commits coldSloadTxs*coldSloadPerTx slots of one contract to MDBX,
// every other one absent, and reads them through a fresh SharedDomains so no read hits the mem batch.
func newColdSloadBlock(tb testing.TB) *coldSloadBlock {
	_, tx, domains := NewTestRwTx(tb)
	addr := accounts.InternAddress([20]byte{0xc0, 0x1d})
	slots := make([]accounts.StorageKey, coldSloadTxs*coldSloadPerTx)
	w := NewWriter(domains.AsPutDel(tx), nil, 1)
	for i := range slots {
		var k [32]byte
		binary.BigEndian.PutUint64(k[24:], uint64(i)*0x9e3779b97f4a7c15)
		slots[i] = accounts.InternKey(k)
		if i%2 == 0 {
			require.NoError(tb, w.WriteAccountStorage(addr, 0, slots[i], uint256.Int{}, *uint256.NewInt(uint64(i) + 1)))
		}
	}
	require.NoError(tb, domains.Flush(tb.Context(), tx))
	domains.Close()
	fresh, err := execctx.NewSharedDomains(tb.Context(), tx, log.New())
	require.NoError(tb, err)
	tb.Cleanup(fresh.Close)
	ibs := New(NewReaderV3(fresh.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	tb.Cleanup(ibs.Close)
	return &coldSloadBlock{ibs: ibs, addr: addr, slots: slots}
}

// run executes one block: each tx reads its own cold slots, one of them twice, then
// hot slots tx 0 published as origins. visit sees every tx's values and read set.
func (blk *coldSloadBlock) run(tb testing.TB, visit func(tx int, vals []uint256.Int, reads ReadSet)) *VersionMap {
	vm := NewVersionMap(nil)
	if blk.touched {
		vm.WriteStorage(blk.addr, accounts.InternKey([32]byte{0xff}), Version{TxIndex: 0}, *uint256.NewInt(1), true)
	}
	vals := make([]uint256.Int, 0, coldSloadPerTx+1+coldSloadHotPerTx)
	for txIdx := range coldSloadTxs {
		blk.ibs.Reset()
		blk.ibs.SetVersionMap(vm)
		blk.ibs.SetTxContext(1, txIdx)
		vals = vals[:0]
		own := blk.slots[txIdx*coldSloadPerTx : (txIdx+1)*coldSloadPerTx]
		for _, k := range own {
			v, err := blk.ibs.GetState(blk.addr, k)
			if err != nil {
				tb.Fatal(err)
			}
			vals = append(vals, v)
		}
		v, err := blk.ibs.GetState(blk.addr, own[0])
		if err != nil {
			tb.Fatal(err)
		}
		vals = append(vals, v)
		if txIdx > 0 {
			for _, k := range blk.slots[:coldSloadHotPerTx] {
				v, err := blk.ibs.GetState(blk.addr, k)
				if err != nil {
					tb.Fatal(err)
				}
				vals = append(vals, v)
			}
		}
		blk.ibs.PublishOrigins()
		if visit != nil {
			visit(txIdx, vals, blk.ibs.Reads())
		}
	}
	return vm
}

func coldSloadDigest(tb testing.TB, blk *coldSloadBlock) string {
	h := sha256.New()
	var lines []string
	vm := blk.run(tb, func(txIdx int, vals []uint256.Int, reads ReadSet) {
		fmt.Fprintf(h, "tx %d\n", txIdx)
		for _, v := range vals {
			fmt.Fprintf(h, "%x\n", v.Bytes32())
		}
		lines = lines[:0]
		reads.RangeFullHeaders(func(a accounts.Address, p AccountPath, k accounts.StorageKey, hdr ReadHeader) bool {
			val := ""
			if p == StoragePath {
				tr, _ := reads.GetStorage(a, k)
				val = fmt.Sprintf("%x", tr.Val.Bytes32())
			}
			lines = append(lines, fmt.Sprintf("%x %d %x %d %d.%d %s", a.Value(), p, k.Value(), hdr.Source, hdr.Version.TxIndex, hdr.Version.Incarnation, val))
			return true
		})
		slices.Sort(lines)
		for _, l := range lines {
			fmt.Fprintln(h, l)
		}
	})
	for _, k := range blk.slots {
		v, ok := vm.LoadOriginStorage(blk.addr, k)
		fmt.Fprintf(h, "origin %t %x\n", ok, v.Bytes32())
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// The values, read-set records and published origins of the cold-SLOAD block are pinned,
// so a change to the read path that serves a slot from a different layer must still agree on all three.
// A cell in an unread slot of the contract must not change them either.
func TestColdSloadBlockReadsArePinned(t *testing.T) {
	blk := newColdSloadBlock(t)
	for _, touched := range []bool{false, true} {
		blk.touched = touched
		require.Equal(t, "1e3b7e77ce538c07b5c0614219d1a4ab2d71dac5abadc79c12302c33ab384a73", coldSloadDigest(t, blk), "touched=%t", touched)
	}
}

func BenchmarkColdSloadBlock(b *testing.B) {
	blk := newColdSloadBlock(b)
	sloads := coldSloadTxs*(coldSloadPerTx+1) + (coldSloadTxs-1)*coldSloadHotPerTx
	for _, touched := range []bool{false, true} {
		b.Run(fmt.Sprintf("touched=%t", touched), func(b *testing.B) {
			blk.touched = touched
			b.ReportAllocs()
			for b.Loop() {
				blk.run(b, nil)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*sloads), "ns/sload")
		})
	}
}
