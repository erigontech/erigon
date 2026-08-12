package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// buildWriteSet makes a set shaped like a tx's writes: each address carries an
// account field plus a few storage slots.
func buildWriteSet(addrs, slotsPerAddr int) *WriteSet {
	ws := &WriteSet{}
	for i := 0; i < addrs; i++ {
		a := accounts.InternAddress(common.BigToAddress(common.Big1))
		av := common.HexToAddress(fmt.Sprintf("0x%040x", i+1))
		a = accounts.InternAddress(av)
		ws.SetBalance(a, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: a, Path: BalancePath}, Val: *uint256.NewInt(uint64(i))})
		ws.SetNonce(a, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: a, Path: NoncePath}, Val: uint64(i)})
		for s := 0; s < slotsPerAddr; s++ {
			k := accounts.InternKey(common.HexToHash(fmt.Sprintf("0x%064x", s+1)))
			ws.SetStorage(a, k, &VersionedWrite[uint256.Int]{
				WriteHeader: WriteHeader{Address: a, Path: StoragePath, Key: k}, Val: *uint256.NewInt(uint64(s))})
		}
	}
	return ws
}

func BenchmarkWriteSetReleaseMaps(b *testing.B) {
	for _, c := range []struct{ addrs, slots int }{
		{1, 0}, {10, 2}, {50, 4}, {200, 5}, {500, 10}, {2000, 10},
	} {
		ws := buildWriteSet(c.addrs, c.slots)
		n := ws.Count()
		ws.ReleaseMaps()
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				w := buildWriteSet(c.addrs, c.slots)
				b.StartTimer()
				w.ReleaseMaps()
			}
		})
	}
}
