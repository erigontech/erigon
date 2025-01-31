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
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/erigontech/erigon-lib/common"

	"github.com/stretchr/testify/require"
)

func generateCellRow(tb testing.TB, size int) (row []*cell, bitmap uint16) {
	tb.Helper()

	row = make([]*cell, size)
	var bm uint16
	for i := 0; i < len(row); i++ {
		row[i] = new(cell)
		row[i].hashLen = 32
		n, err := rand.Read(row[i].hash[:])
		require.NoError(tb, err)
		require.EqualValues(tb, row[i].hashLen, n)

		th := rand.Intn(120)
		switch {
		case th > 70:
			n, err = rand.Read(row[i].accountAddr[:])
			require.NoError(tb, err)
			row[i].accountAddrLen = n
		case th > 20 && th <= 70:
			n, err = rand.Read(row[i].storageAddr[:])
			require.NoError(tb, err)
			row[i].storageAddrLen = n
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
	t.Parallel()
	row, bm := generateCellRow(t, 16)

	be := NewBranchEncoder(1024)
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(i int, skip bool) (*cell, error) {
		return row[i], nil
	})

	require.NoError(t, err)
	require.NotEmpty(t, enc)
	t.Logf("enc [%d] %x\n", len(enc), enc)

	bmg := NewHexBranchMerger(8192)
	res, err := bmg.Merge(enc, enc)
	require.NoError(t, err)
	require.EqualValues(t, enc, res)

	tm, am, origins, err := res.decodeCells()
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
		require.EqualValues(t, row[i].accountAddrLen, c.accountAddrLen)
		require.EqualValues(t, row[i].accountAddr, c.accountAddr)
		require.EqualValues(t, row[i].storageAddrLen, c.storageAddrLen)
		require.EqualValues(t, row[i].storageAddr, c.storageAddr)
		i++
	}
}

func TestBranchData_MergeHexBranchesEmptyBranches(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	enc, err := hex.DecodeString(encs)
	require.NoError(t, err)

	//tm, am, origins, err := BranchData(enc).decodeCells()
	require.NoError(t, err)
	t.Logf("%s", BranchData(enc).String())
	//require.EqualValues(t, tm, am)
	//_, _ = tm, am
}

// used as a tool for occasional decode of arbitrary BranchData
func TestDecodeBranchWithLeafHashes(t *testing.T) {
	t.Helper()

	enc := "ffffffff0820c897b4bd17e28055ef895b93b3c9d860f500d5f71cc4d88da43174f21855e7360820de5fad02cc43b2adc2d69d3bcf075c46f94f2e93c99e705e5e38e78b0deecfce08201913bafd6fa83683fc070e3bce0aca7276801fac48b671e85eba2c518afb5b090820e49b6b70a891c92d92018ebea62b10de7e64c91d19b3dd53ab566c2960d02b35082001043b43a93d87d986ff699fc6dcbfc24bb1e4bb5de01e1a873a31376ec2f362082094854c60e0b479a4fe32338da59c19f9fa678c98ac279310de541c406f78f7d208200a47e9bedcd30cb8445b11a701dc895ff6935a3b4d03818cd34bccd91da77cab0820da84ca186cd0ef061b53e2f8dde6f63ce926ef53dd238f6421f7e9a518d3b1ee0820b33cc57f00e8dbf97acf4f8d53f450cd305f126caa3aaec39f1a2c26ac53e87d0820240ec57bdbed7448939cf084698c33037c0f7333bdd6ef8a5b9cb16e2ad4e26f08205c3b6e60f80a65ca3feeec5e5c79aa7861cd37de95af2a9909c39ee5c276d5660820e76fd92681b17a61d625b429c54e4d94b90a0e377795e0f08fe006b66e918bb808204049fddbeb5275fae0e3e0a4d91fbe85f903326727bf99a023c67a8a136ec01d0820be379602300baaa9ff56feb0ad049ed5236dfbe8d1bb0d16209209b9910d58520820c4d66181775ac0addfaaf11e24accd8a12e9cd59d994d64476b4f8057e0081e208202b09e493d0eb47fbc7d2c9acf8c5c7b47a771ec20ea263e44513aa699a254350"
	enc1 := "0520ffff0820c897b4bd17e28055ef895b93b3c9d860f500d5f71cc4d88da43174f21855e7360820de5fad02cc43b2adc2d69d3bcf075c46f94f2e93c99e705e5e38e78b0deecfce08201913bafd6fa83683fc070e3bce0aca7276801fac48b671e85eba2c518afb5b090820e49b6b70a891c92d92018ebea62b10de7e64c91d19b3dd53ab566c2960d02b35082001043b43a93d87d986ff699fc6dcbfc24bb1e4bb5de01e1a873a31376ec2f362082094854c60e0b479a4fe32338da59c19f9fa678c98ac279310de541c406f78f7d208200a47e9bedcd30cb8445b11a701dc895ff6935a3b4d03818cd34bccd91da77cab0820da84ca186cd0ef061b53e2f8dde6f63ce926ef53dd238f6421f7e9a518d3b1ee0820b33cc57f00e8dbf97acf4f8d53f450cd305f126caa3aaec39f1a2c26ac53e87d0820240ec57bdbed7448939cf084698c33037c0f7333bdd6ef8a5b9cb16e2ad4e26f08205c3b6e60f80a65ca3feeec5e5c79aa7861cd37de95af2a9909c39ee5c276d5660820e76fd92681b17a61d625b429c54e4d94b90a0e377795e0f08fe006b66e918bb808204049fddbeb5275fae0e3e0a4d91fbe85f903326727bf99a023c67a8a136ec01d0820be379602300baaa9ff56feb0ad049ed5236dfbe8d1bb0d16209209b9910d58520820c4d66181775ac0addfaaf11e24accd8a12e9cd59d994d64476b4f8057e0081e208202b09e493d0eb47fbc7d2c9acf8c5c7b47a771ec20ea263e44513aa699a254350"
	unfoldBranchDataFromString(t, enc)
	unfoldBranchDataFromString(t, enc1)
}

// helper to decode row of cells from string
func unfoldBranchDataFromString(tb testing.TB, encs string) (row []*cell, am uint16) {
	tb.Helper()

	//encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	//encs := "37ad10eb75ea0fc1c363db0dda0cd2250426ee2c72787155101ca0e50804349a94b649deadcc5cddc0d2fd9fb358c2edc4e7912d165f88877b1e48c69efacf418e923124506fbb2fd64823fd41cbc10427c423"
	enc, err := hex.DecodeString(encs)
	require.NoError(tb, err)

	tm, am, origins, err := BranchData(enc).decodeCells()
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
	t.Parallel()

	row, bm := generateCellRow(t, 16)

	cells, am := unfoldBranchDataFromString(t, "86e586e5082035e72a782b51d9c98548467e3f868294d923cdbbdf4ce326c867bd972c4a2395090109203b51781a76dc87640aea038e3fdd8adca94049aaa436735b162881ec159f6fb408201aa2fa41b5fb019e8abf8fc32800805a2743cfa15373cf64ba16f4f70e683d8e0404a192d9050404f993d9050404e594d90508208642542ff3ce7d63b9703e85eb924ab3071aa39c25b1651c6dda4216387478f10404bd96d905")
	for i, c := range cells {
		if c == nil {
			continue
		}
		if c.accountAddrLen > 0 {
			offt, _ := binary.Uvarint(c.accountAddr[:c.accountAddrLen])
			t.Logf("%d apk %x, offt %d\n", i, c.accountAddr[:c.accountAddrLen], offt)
		}
		if c.storageAddrLen > 0 {
			offt, _ := binary.Uvarint(c.storageAddr[:c.storageAddrLen])
			t.Logf("%d spk %x offt %d\n", i, c.storageAddr[:c.storageAddrLen], offt)
		}

	}
	_ = cells
	_ = am

	cg := func(nibble int, skip bool) (*cell, error) {
		return row[nibble], nil
	}

	be := NewBranchEncoder(1024)
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
	t.Parallel()

	row, bm := generateCellRow(t, 16)

	cg := func(nibble int, skip bool) (*cell, error) {
		return row[nibble], nil
	}

	be := NewBranchEncoder(1024)
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
	t.Parallel()

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
	t.Parallel()

	utUpdate := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utDirect := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	type tc struct {
		key []byte
		val []byte
	}

	upds := []tc{
		{common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"), []byte("value1")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"), []byte("value0")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("2452345febefe553bba1d92398a69fbc9f01593b"), []byte("value8")},
		{common.FromHex("ffffffffffff8a69fbc9f01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbceeeeeeeee66"), []byte("value8")},
		{common.FromHex("553bba1d9239aaaaaaaaa01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d92398a69fbc9f01593bb777777777777"), []byte("value8")},
		{common.FromHex("5cccccccccccca69fbc9f01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d92398a69fbc9feeeeeeee51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d9bbbbbbbbbbbbb1593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d9ffffffffffff01593bbc51b5aaaaaaa"), []byte("value8")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7f1425f7d8358332af195"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f881055fffffffff1425f7d8358332af195"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7eeeeeeeeeeeeeeeeee95"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974aaaaaaa1f81dfb7f1425f7d8358332af195"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7f1425f7d835838888885"), []byte("value1")},
	}
	for i := 0; i < len(upds); i++ {
		utUpdate.TouchPlainKey(string(upds[i].key), upds[i].val, utUpdate.TouchStorage)
		utDirect.TouchPlainKey(string(upds[i].key), upds[i].val, utDirect.TouchStorage)
	}

	uniqUpds := make(map[string]tc)
	for i := 0; i < len(upds); i++ {
		if _, exist := uniqUpds[string(upds[i].key)]; exist {
			fmt.Printf("deduped %x\n", upds[i].key)
		}
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
	require.EqualValues(t, len(uniqUpds), sz)

	sz = utDirect.Size()
	require.EqualValues(t, len(uniqUpds), sz)

	i := 0
	// keyHasherNoop is used so ordering is going by plainKey
	err := utUpdate.HashSort(context.Background(), func(hk, pk []byte, upd *Update) error {
		require.EqualValues(t, sortedUniqUpds[i].key, pk)
		require.EqualValues(t, sortedUniqUpds[i].val, upd.Storage[:upd.StorageLen])
		i++
		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, len(uniqUpds), i)

	i = 0
	err = utDirect.HashSort(context.Background(), func(hk, pk []byte, _ *Update) error {
		require.EqualValues(t, sortedUniqUpds[i].key, pk)
		i++
		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, len(uniqUpds), i)
}
