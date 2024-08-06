// Copyright 2024 The Erigon Authors
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

package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"sort"
	"testing"

	"github.com/erigontech/erigon-lib/common"

	"github.com/stretchr/testify/require"
)

func generateCellRow(tb testing.TB, size int) (row []*Cell, bitmap uint16) {
	tb.Helper()

	row = make([]*Cell, size)
	var bm uint16
	for i := 0; i < len(row); i++ {
		row[i] = new(Cell)
		row[i].HashLen = 32
		n, err := rand.Read(row[i].hash[:])
		require.NoError(tb, err)
		require.EqualValues(tb, row[i].HashLen, n)

		th := rand.Intn(120)
		switch {
		case th > 70:
			n, err = rand.Read(row[i].accountPlainKey[:])
			require.NoError(tb, err)
			row[i].accountPlainKeyLen = n
		case th > 20 && th <= 70:
			n, err = rand.Read(row[i].storagePlainKey[:])
			require.NoError(tb, err)
			row[i].storagePlainKeyLen = n
		case th <= 20:
			n, err = rand.Read(row[i].extension[:th])
			row[i].extLen = n
			require.NoError(tb, err)
			require.EqualValues(tb, th, n)
		}
		bm |= uint16(1 << i)
	}
	return row, bm
}

func TestBranchData_MergeHexBranches2(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	be := NewBranchEncoder(1024, t.TempDir())
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(i int, skip bool) (*Cell, error) {
		return row[i], nil
	})

	require.NoError(t, err)
	require.NotEmpty(t, enc)
	t.Logf("enc [%d] %x\n", len(enc), enc)

	bmg := NewHexBranchMerger(8192)
	res, err := bmg.Merge(enc, enc)
	require.NoError(t, err)
	require.EqualValues(t, enc, res)

	tm, am, origins, err := res.DecodeCells()
	require.NoError(t, err)
	require.EqualValues(t, tm, am)
	require.EqualValues(t, bm, am)

	i := 0
	for _, c := range origins {
		if c == nil {
			continue
		}
		require.EqualValues(t, row[i].extLen, c.extLen)
		require.EqualValues(t, row[i].extension, c.extension)
		require.EqualValues(t, row[i].accountPlainKeyLen, c.accountPlainKeyLen)
		require.EqualValues(t, row[i].accountPlainKey, c.accountPlainKey)
		require.EqualValues(t, row[i].storagePlainKeyLen, c.storagePlainKeyLen)
		require.EqualValues(t, row[i].storagePlainKey, c.storagePlainKey)
		i++
	}
}

func TestBranchData_MergeHexBranches_ValueAliveAfterNewMerges(t *testing.T) {
	t.Skip()
	row, bm := generateCellRow(t, 16)

	be := NewBranchEncoder(1024, t.TempDir())
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(i int, skip bool) (*Cell, error) {
		return row[i], nil
	})
	require.NoError(t, err)

	copies := make([][]byte, 16)
	values := make([][]byte, len(copies))

	merger := NewHexBranchMerger(8192)

	var tm uint16
	am := bm

	for i := 15; i >= 0; i-- {
		row[i] = nil
		tm, bm, am = uint16(1<<i), bm>>1, am>>1
		enc1, _, err := be.EncodeBranch(bm, tm, am, func(i int, skip bool) (*Cell, error) {
			return row[i], nil
		})
		require.NoError(t, err)
		merged, err := merger.Merge(enc, enc1)
		require.NoError(t, err)

		copies[i] = common.Copy(merged)
		values[i] = merged
	}
	for i := 0; i < len(copies); i++ {
		require.EqualValues(t, copies[i], values[i])
	}
}

func TestBranchData_MergeHexBranchesEmptyBranches(t *testing.T) {
	// Create a BranchMerger instance with sufficient capacity for testing.
	merger := NewHexBranchMerger(1024)

	// Test merging when one branch is empty.
	branch1 := BranchData{}
	branch2 := BranchData{0x02, 0x02, 0x03, 0x03, 0x0C, 0x02, 0x04, 0x0C}
	mergedBranch, err := merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch2, mergedBranch)

	// Test merging when both branches are empty.
	branch1 = BranchData{}
	branch2 = BranchData{}
	mergedBranch, err = merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch1, mergedBranch)
}

// Additional tests for error cases, edge cases, and other scenarios can be added here.

func TestBranchData_MergeHexBranches3(t *testing.T) {
	encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	enc, err := hex.DecodeString(encs)
	require.NoError(t, err)

	//tm, am, origins, err := BranchData(enc).DecodeCells()
	require.NoError(t, err)
	t.Logf("%s", BranchData(enc).String())
	//require.EqualValues(t, tm, am)
	//_, _ = tm, am
}

// helper to decode row of cells from string
func unfoldBranchDataFromString(tb testing.TB, encs string) (row []*Cell, am uint16) {
	tb.Helper()

	//encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	//encs := "37ad10eb75ea0fc1c363db0dda0cd2250426ee2c72787155101ca0e50804349a94b649deadcc5cddc0d2fd9fb358c2edc4e7912d165f88877b1e48c69efacf418e923124506fbb2fd64823fd41cbc10427c423"
	enc, err := hex.DecodeString(encs)
	require.NoError(tb, err)

	tm, am, origins, err := BranchData(enc).DecodeCells()
	require.NoError(tb, err)
	_, _ = tm, am

	tb.Logf("%s", BranchData(enc).String())
	//require.EqualValues(tb, tm, am)
	//for i, c := range origins {
	//	if c == nil {
	//		continue
	//	}
	//	fmt.Printf("i %d, c %#+v\n", i, c)
	//}
	return origins[:], am
}

func TestBranchData_ReplacePlainKeys(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	cells, am := unfoldBranchDataFromString(t, "86e586e5082035e72a782b51d9c98548467e3f868294d923cdbbdf4ce326c867bd972c4a2395090109203b51781a76dc87640aea038e3fdd8adca94049aaa436735b162881ec159f6fb408201aa2fa41b5fb019e8abf8fc32800805a2743cfa15373cf64ba16f4f70e683d8e0404a192d9050404f993d9050404e594d90508208642542ff3ce7d63b9703e85eb924ab3071aa39c25b1651c6dda4216387478f10404bd96d905")
	for i, c := range cells {
		if c == nil {
			continue
		}
		if c.accountPlainKeyLen > 0 {
			offt, _ := binary.Uvarint(c.accountPlainKey[:c.accountPlainKeyLen])
			t.Logf("%d apk %x, offt %d\n", i, c.accountPlainKey[:c.accountPlainKeyLen], offt)
		}
		if c.storagePlainKeyLen > 0 {
			offt, _ := binary.Uvarint(c.storagePlainKey[:c.storagePlainKeyLen])
			t.Logf("%d spk %x offt %d\n", i, c.storagePlainKey[:c.storagePlainKeyLen], offt)
		}

	}
	_ = cells
	_ = am

	cg := func(nibble int, skip bool) (*Cell, error) {
		return row[nibble], nil
	}

	be := NewBranchEncoder(1024, t.TempDir())
	enc, _, err := be.EncodeBranch(bm, bm, bm, cg)
	require.NoError(t, err)

	original := common.Copy(enc)

	target := make([]byte, 0, len(enc))
	oldKeys := make([][]byte, 0)
	replaced, err := enc.ReplacePlainKeys(target, func(key []byte, isStorage bool) ([]byte, error) {
		oldKeys = append(oldKeys, key)
		if isStorage {
			return key[:8], nil
		}
		return key[:4], nil
	})
	require.NoError(t, err)
	require.Truef(t, len(replaced) < len(enc), "replaced expected to be shorter than original enc")

	keyI := 0
	replacedBack, err := replaced.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		require.EqualValues(t, oldKeys[keyI][:4], key[:4])
		defer func() { keyI++ }()
		return oldKeys[keyI], nil
	})
	require.NoError(t, err)
	require.EqualValues(t, original, replacedBack)

	t.Run("merge replaced and original back", func(t *testing.T) {
		orig := common.Copy(original)

		merged, err := replaced.MergeHexBranches(original, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)

		merged, err = merged.MergeHexBranches(replacedBack, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)
	})
}

func TestBranchData_ReplacePlainKeys_WithEmpty(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	cg := func(nibble int, skip bool) (*Cell, error) {
		return row[nibble], nil
	}

	be := NewBranchEncoder(1024, t.TempDir())
	enc, _, err := be.EncodeBranch(bm, bm, bm, cg)
	require.NoError(t, err)

	original := common.Copy(enc)

	target := make([]byte, 0, len(enc))
	oldKeys := make([][]byte, 0)
	replaced, err := enc.ReplacePlainKeys(target, func(key []byte, isStorage bool) ([]byte, error) {
		oldKeys = append(oldKeys, key)
		if isStorage {
			return nil, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	require.EqualValuesf(t, len(enc), len(replaced), "replaced expected to be equal to origin (since no replacements were made)")

	keyI := 0
	replacedBack, err := replaced.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		require.EqualValues(t, oldKeys[keyI][:4], key[:4])
		defer func() { keyI++ }()
		return oldKeys[keyI], nil
	})
	require.NoError(t, err)
	require.EqualValues(t, original, replacedBack)

	t.Run("merge replaced and original back", func(t *testing.T) {
		orig := common.Copy(original)

		merged, err := replaced.MergeHexBranches(original, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)

		merged, err = merged.MergeHexBranches(replacedBack, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)
	})
}

func TestNewUpdates(t *testing.T) {
	t.Run("ModeUpdate", func(t *testing.T) {
		ut := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)

		require.NotNil(t, ut.tree)
		require.NotNil(t, ut.keccak)
		require.Nil(t, ut.keys)
		require.Equal(t, ModeUpdate, ut.mode)
	})

	t.Run("ModeDirect", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

		require.NotNil(t, ut.keccak)
		require.NotNil(t, ut.keys)
		require.Equal(t, ModeDirect, ut.mode)
	})

}

func TestUpdates_TouchPlainKey(t *testing.T) {
	utUpdate := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utDirect := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	utUpdate1 := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utDirect1 := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	type tc struct {
		key []byte
		val []byte
	}

	upds := []tc{
		{common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"), []byte("value1")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"), []byte("value0")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7f1425f7d8358332af195"), []byte("value1")},
	}
	for i := 0; i < len(upds); i++ {
		utUpdate.TouchPlainKey(upds[i].key, upds[i].val, utUpdate.TouchStorage)
		utDirect.TouchPlainKey(upds[i].key, upds[i].val, utDirect.TouchStorage)
		utUpdate1.TouchPlainKey(upds[i].key, upds[i].val, utUpdate.TouchStorage)
		utDirect1.TouchPlainKey(upds[i].key, upds[i].val, utDirect.TouchStorage)
	}

	uniqUpds := make(map[string]tc)
	for i := 0; i < len(upds); i++ {
		uniqUpds[string(upds[i].key)] = upds[i]
	}
	sortedUniqUpds := make([]tc, 0, len(uniqUpds))
	for _, v := range uniqUpds {
		sortedUniqUpds = append(sortedUniqUpds, v)
	}
	sort.Slice(sortedUniqUpds, func(i, j int) bool {
		return bytes.Compare(sortedUniqUpds[i].key, sortedUniqUpds[j].key) < 0
	})

	sz := utUpdate.Size()
	require.EqualValues(t, 3, sz)

	sz = utDirect.Size()
	require.EqualValues(t, 3, sz)

	pk, upd := utUpdate.List(true)
	require.Len(t, pk, 3)
	require.NotNil(t, upd)

	for i := 0; i < len(sortedUniqUpds); i++ {
		require.EqualValues(t, sortedUniqUpds[i].key, pk[i])
		require.EqualValues(t, sortedUniqUpds[i].val, upd[i].CodeHashOrStorage[:upd[i].ValLength])
	}

	pk, upd = utDirect.List(true)
	require.Len(t, pk, 3)
	require.Nil(t, upd)

	for i := 0; i < len(sortedUniqUpds); i++ {
		require.EqualValues(t, sortedUniqUpds[i].key, pk[i])
	}

	i := 0
	err := utUpdate1.HashSort(context.Background(), func(hk, pk []byte) error {
		require.EqualValues(t, sortedUniqUpds[i].key, pk)
		i++
		return nil
	})
	require.NoError(t, err)

	i = 0
	err = utDirect1.HashSort(context.Background(), func(hk, pk []byte) error {
		require.EqualValues(t, sortedUniqUpds[i].key, pk)
		i++
		return nil
	})
	require.NoError(t, err)
}
