package commitment

import (
	"encoding/binary"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkBranchMerger_Merge(b *testing.B) {
	b.StopTimer()
	row, bm := generateCellRow(b, 16)

	be := NewBranchEncoder(1024, b.TempDir())
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(i int, skip bool) (*Cell, error) {
		return row[i], nil
	})
	require.NoError(b, err)

	var copies [16][]byte
	var tm uint16
	am := bm

	for i := 15; i >= 0; i-- {
		row[i] = nil
		tm, bm, am = uint16(1<<i), bm>>1, am>>1
		enc1, _, err := be.EncodeBranch(bm, tm, am, func(i int, skip bool) (*Cell, error) {
			return row[i], nil
		})
		require.NoError(b, err)

		copies[i] = common.Copy(enc1)
	}

	b.StartTimer()
	bmg := NewHexBranchMerger(4096)
	var ci int
	for i := 0; i < b.N; i++ {
		_, err := bmg.Merge(enc, copies[ci])
		if err != nil {
			b.Fatal(err)
		}
		ci++
		if ci == len(copies) {
			ci = 0
		}
	}
}

func BenchmarkBranchData_ReplacePlainKeys(b *testing.B) {
	row, bm := generateCellRow(b, 16)

	cells, am := unfoldBranchDataFromString(b, "86e586e5082035e72a782b51d9c98548467e3f868294d923cdbbdf4ce326c867bd972c4a2395090109203b51781a76dc87640aea038e3fdd8adca94049aaa436735b162881ec159f6fb408201aa2fa41b5fb019e8abf8fc32800805a2743cfa15373cf64ba16f4f70e683d8e0404a192d9050404f993d9050404e594d90508208642542ff3ce7d63b9703e85eb924ab3071aa39c25b1651c6dda4216387478f10404bd96d905")
	for i, c := range cells {
		if c == nil {
			continue
		}
		if c.apl > 0 {
			offt, _ := binary.Uvarint(c.apk[:c.apl])
			b.Logf("%d apk %x, offt %d\n", i, c.apk[:c.apl], offt)
		}
		if c.spl > 0 {
			offt, _ := binary.Uvarint(c.spk[:c.spl])
			b.Logf("%d spk %x offt %d\n", i, c.spk[:c.spl], offt)
		}

	}
	_ = cells
	_ = am

	cg := func(nibble int, skip bool) (*Cell, error) {
		return row[nibble], nil
	}

	be := NewBranchEncoder(1024, b.TempDir())
	enc, _, err := be.EncodeBranch(bm, bm, bm, cg)
	require.NoError(b, err)

	original := common.Copy(enc)
	for i := 0; i < b.N; i++ {
		target := make([]byte, 0, len(enc))
		oldKeys := make([][]byte, 0)
		replaced, err := enc.ReplacePlainKeys(target, func(key []byte, isStorage bool) ([]byte, error) {
			oldKeys = append(oldKeys, key)
			if isStorage {
				return key[:8], nil
			}
			return key[:4], nil
		})
		require.NoError(b, err)
		require.Truef(b, len(replaced) < len(enc), "replaced expected to be shorter than original enc")

		keyI := 0
		replacedBack, err := replaced.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			require.EqualValues(b, oldKeys[keyI][:4], key[:4])
			defer func() { keyI++ }()
			return oldKeys[keyI], nil
		})
		require.NoError(b, err)
		require.EqualValues(b, original, replacedBack)
	}
}
