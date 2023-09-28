package commitment

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func generateCellRow(t *testing.T, size int) (row []*Cell, bitmap uint16) {
	t.Helper()

	row = make([]*Cell, size)
	var bm uint16
	for i := 0; i < len(row); i++ {
		row[i] = new(Cell)
		row[i].hl = 32
		n, err := rand.Read(row[i].h[:])
		require.NoError(t, err)
		require.EqualValues(t, row[i].hl, n)

		th := rand.Intn(120)
		switch {
		case th > 70:
			n, err = rand.Read(row[i].apk[:])
			require.NoError(t, err)
			row[i].apl = n
		case th > 20 && th <= 70:
			n, err = rand.Read(row[i].spk[:])
			require.NoError(t, err)
			row[i].spl = n
		case th <= 20:
			n, err = rand.Read(row[i].extension[:th])
			row[i].extLen = n
			require.NoError(t, err)
			require.EqualValues(t, th, n)
		}
		bm |= uint16(1 << i)
	}
	return row, bm
}

func TestBranchData_MergeHexBranches2(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	enc, _, err := EncodeBranch(bm, bm, bm, func(i int, skip bool) (*Cell, error) {
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
		require.EqualValues(t, row[i].apl, c.apl)
		require.EqualValues(t, row[i].apk, c.apk)
		require.EqualValues(t, row[i].spl, c.spl)
		require.EqualValues(t, row[i].spk, c.spk)
		i++
	}
}

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
func Test_UTIL_UnfoldBranchDataFromString(t *testing.T) {
	t.Skip()

	//encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	encs := "37ad10eb75ea0fc1c363db0dda0cd2250426ee2c72787155101ca0e50804349a94b649deadcc5cddc0d2fd9fb358c2edc4e7912d165f88877b1e48c69efacf418e923124506fbb2fd64823fd41cbc10427c423"
	enc, err := hex.DecodeString(encs)
	require.NoError(t, err)

	bfn := func(pref []byte) ([]byte, error) {
		return enc, nil
	}
	sfn := func(pref []byte, c *Cell) error {
		return nil
	}

	hph := NewHexPatriciaHashed(20, bfn, nil, sfn)
	hph.unfoldBranchNode(1, false, 0)
	tm, am, origins, err := BranchData(enc).DecodeCells()
	require.NoError(t, err)
	t.Logf("%s", BranchData(enc).String())
	//require.EqualValues(t, tm, am)
	_, _ = tm, am

	i := 0
	for _, c := range origins {
		if c == nil {
			continue
		}
		fmt.Printf("i %d, c %#+v\n", i, c)
		i++
	}
}

func TestBranchData_ExtractPlainKeys(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	cg := func(nibble int, skip bool) (*Cell, error) {
		return row[nibble], nil
	}

	enc, _, err := EncodeBranch(bm, bm, bm, cg)
	require.NoError(t, err)

	extAPK, extSPK, err := enc.ExtractPlainKeys()
	require.NoError(t, err)

	for i, c := range row {
		if c == nil {
			continue
		}
		switch {
		case c.apl != 0:
			require.Containsf(t, extAPK, c.apk[:], "at pos %d expected %x..", i, c.apk[:8])
		case c.spl != 0:
			require.Containsf(t, extSPK, c.spk[:], "at pos %d expected %x..", i, c.spk[:8])
		default:
			continue
		}
	}
}

func TestBranchData_ReplacePlainKeys(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	cg := func(nibble int, skip bool) (*Cell, error) {
		return row[nibble], nil
	}

	enc, _, err := EncodeBranch(bm, bm, bm, cg)
	require.NoError(t, err)

	extAPK, extSPK, err := enc.ExtractPlainKeys()
	require.NoError(t, err)

	shortApk, shortSpk := make([][]byte, 0), make([][]byte, 0)
	for i, c := range row {
		if c == nil {
			continue
		}
		switch {
		case c.apl != 0:
			shortApk = append(shortApk, c.apk[:8])
			require.Containsf(t, extAPK, c.apk[:], "at pos %d expected %x..", i, c.apk[:8])
		case c.spl != 0:
			shortSpk = append(shortSpk, c.spk[:8])
			require.Containsf(t, extSPK, c.spk[:], "at pos %d expected %x..", i, c.spk[:8])
		default:
			continue
		}
	}

	target := make([]byte, 0, len(enc))
	replaced, err := enc.ReplacePlainKeys(shortApk, shortSpk, target)
	require.NoError(t, err)
	require.Truef(t, len(replaced) < len(enc), "replaced expected to be shorter than original enc")

	rextA, rextS, err := replaced.ExtractPlainKeys()
	require.NoError(t, err)

	for _, apk := range shortApk {
		require.Containsf(t, rextA, apk, "expected %x to be in replaced account keys", apk)
	}
	for _, spk := range shortSpk {
		require.Containsf(t, rextS, spk, "expected %x to be in replaced storage keys", spk)
	}
	require.True(t, len(shortApk) == len(rextA))
	require.True(t, len(shortSpk) == len(rextS))
}
