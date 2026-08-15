package state

import "testing"

func TestPushSnapshotHotPathNoAllocs(t *testing.T) {
	construct := testing.AllocsPerRun(200, func() {
		ibs := New(nil)
		_ = ibs
	})
	constructAndPush := testing.AllocsPerRun(200, func() {
		ibs := New(nil)
		var ids [16]int
		for i := range ids {
			ids[i] = ibs.PushSnapshot()
		}
		for i := len(ids) - 1; i >= 0; i-- {
			ibs.PopSnapshot(ids[i])
		}
	})
	if delta := constructAndPush - construct; delta >= 1 {
		t.Fatalf("16 PushSnapshots on a fresh IntraBlockState allocated %.1f times", delta)
	}
}
