package stateless

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"math"
	"math/big"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dimg"
	"github.com/petar/GoLLRB/llrb"
	//"sort"
)

type KeyItem struct {
	key common.Hash
}

func (a *KeyItem) Less(b llrb.Item) bool {
	bi := b.(*KeyItem)
	return bytes.Compare(a.key[:], bi.key[:]) < 0
}

//nolint
func storageRoot(db ethdb.KV, contract common.Address) (common.Hash, error) {
	var storageRoot common.Hash
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		enc, err := tx.GetOne(dbutils.TrieOfStorageBucket, crypto.Keccak256(contract[:]))
		if err != nil {
			return err
		}
		if enc == nil {
			return fmt.Errorf("could find account %x", contract)
		}
		storageRoot = common.BytesToHash(common.CopyBytes(enc))
		return nil
	}); err != nil {
		return storageRoot, err
	}
	return storageRoot, nil
}

//nolint
func actualContractSize(db ethdb.KV, contract common.Address) (int, error) {
	var fk [52]byte
	copy(fk[:], contract[:])
	actual := 0
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HashedStorageBucket)
		for k, _, err := c.Seek(fk[:]); k != nil && bytes.HasPrefix(k, contract[:]); k, _, err = c.Next() {
			if err != nil {
				return err
			}
			actual++
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return actual, nil
}

func estimateContractSize(seed common.Hash, current *llrb.LLRB, probes int, probeWidth int, trace bool) (int, error) {
	if trace {
		fmt.Printf("-----------------------------\n")
	}
	var seekkey KeyItem
	var large [33]byte
	large[0] = 1
	largeInt := big.NewInt(0).SetBytes(large[:])
	sectorSize := big.NewInt(0).Div(largeInt, big.NewInt(int64(probes)))
	probeKeyHash := seed[:]
	probe := big.NewInt(0).SetBytes(seed[:])
	samples := make(map[[32]byte]*big.Int)
	curr := big.NewInt(0)
	prev := big.NewInt(0)
	allSteps := big.NewInt(0)

	for i := 0; i < probes; i++ {
		if trace {
			fmt.Printf("i==%d\n", i)
		}
		prev.SetBytes(probeKeyHash)
		allSteps.SetUint64(0)
		for ci := 0; ci < 32-len(probeKeyHash); ci++ {
			seekkey.key[ci] = 0
		}
		copy(seekkey.key[32-len(probeKeyHash):], probeKeyHash)
		if trace {
			fmt.Printf("seekkey: %x\n", seekkey.key)
		}
		var firstK *KeyItem
		for j := 0; j <= probeWidth; {
			if trace {
				fmt.Printf("Start with j == %d\n", j)
			}
			current.AscendGreaterOrEqual(&seekkey, func(item llrb.Item) bool {
				if j > probeWidth {
					return false
				}
				k := item.(*KeyItem)
				if trace {
					fmt.Printf("j == %d, %x\n", j, k.key)
				}
				curr.SetBytes(k.key[:])
				diff := big.NewInt(0)
				if prev.Cmp(curr) < 0 {
					diff.Sub(curr, prev)
				} else {
					diff.Sub(prev, curr)
					diff.Sub(largeInt, diff)
				}
				allSteps.Add(allSteps, diff)
				if _, ok := samples[k.key]; !ok || j > 0 {
					samples[k.key] = diff
				}
				prev.SetBytes(k.key[:])
				j++
				if firstK == nil {
					firstK = k
				} else if k == firstK {
					j = probeWidth + 1
				}
				return true
			})
			if j <= probeWidth {
				for ci := 0; ci < 32; ci++ {
					seekkey.key[ci] = 0
				}
				if trace {
					fmt.Printf("Looping at j == %d\n", j)
				}
			}
		}
		if allSteps.Cmp(sectorSize) < 0 {
			probe.Add(probe, sectorSize)
		} else {
			if trace {
				fmt.Printf("Move by allSteps\n")
			}
			probe.Add(probe, allSteps)
		}
		for probe.Cmp(largeInt) >= 0 {
			probe.Sub(probe, largeInt)
		}
		probeKeyHash = probe.Bytes()
	}
	total := big.NewInt(0)
	for _, sample := range samples {
		total.Add(total, sample)
	}
	sampleCount := len(samples)
	estimatedInt := big.NewInt(0)
	if sampleCount > 0 {
		estimatedInt.Mul(largeInt, big.NewInt(int64(sampleCount)))
		estimatedInt.Div(estimatedInt, total)
	}
	if trace {
		fmt.Printf("probes: %d, probeWidth: %d, sampleCount: %d, estimate: %d\n", probes, probeWidth, sampleCount, estimatedInt)
	}
	return int(estimatedInt.Int64()), nil
}

func getHeatMapColor(value float64) (red, green, blue float64) {
	const NUM_COLORS int = 4
	color := [NUM_COLORS][3]float64{
		{0, 0, 1},
		{0, 1, 0},
		{1, 1, 0},
		{1, 0, 0},
	}
	// A static array of 4 colors:  (blue,   green,  yellow,  red) using {r,g,b} for each.

	var idx1 int             // |-- Our desired color will be between these two indexes in "color".
	var idx2 int             // |
	var fractBetween float64 // Fraction between "idx1" and "idx2" where our value is.

	if value <= 0 {
		idx1 = 0
		idx2 = 0
	} else if value >= 1 {
		idx1 = NUM_COLORS - 1
		idx2 = NUM_COLORS - 1
	} else {
		value = value * float64(NUM_COLORS-1) // Will multiply value by 3.
		idx1 = int(value)                     // Our desired color will be after this index.
		idx2 = idx1 + 1                       // ... and before this index (inclusive).
		fractBetween = value - float64(idx1)  // Distance between the two indexes (0-1).
	}

	if idx1 >= len(color) || idx1 < 0 {
		fmt.Printf("value: %f, idx1: %d\n", value, idx1)
	}
	if idx2 >= len(color) || idx2 < 0 {
		fmt.Printf("value: %f, idx2: %d\n", value, idx2)
	}
	red = (color[idx2][0]-color[idx1][0])*fractBetween + color[idx1][0]
	green = (color[idx2][1]-color[idx1][1])*fractBetween + color[idx1][1]
	blue = (color[idx2][2]-color[idx1][2])*fractBetween + color[idx1][2]
	return
}

func estimateContract(
	idx int,
	current *llrb.LLRB,
	seed common.Hash,
	valMap map[int][][]float64,
	maxValMap map[int][][]float64,
	valCount map[int]int,
	maxi, maxj int,
	trace bool,
) bool {
	maxAllVals := maxValMap[0]
	allVals := valMap[0]
	actual := current.Len()
	if trace {
		fmt.Printf("Actual size: %d\n", actual)
	}
	if actual < 2 {
		return false
	}
	category := int(math.Log2(float64(actual)))
	if category != 1 {
		//return false
	}
	//fmt.Printf("%d\n", idx)
	maxVals, ok := maxValMap[category]
	if !ok {
		maxVals = make([][]float64, maxi)
		for i := 1; i < maxi; i++ {
			maxVals[i] = make([]float64, maxj)
		}
		maxValMap[category] = maxVals
	}
	vals, ok := valMap[category]
	if !ok {
		vals = make([][]float64, maxi)
		for i := 1; i < maxi; i++ {
			vals[i] = make([]float64, maxj)
		}
		valMap[category] = vals
	}
	for i := 1; i < maxi; i++ {
		for j := 1; j < maxj; j++ {
			estimated, err := estimateContractSize(seed, current, i, j, trace)
			check(err)
			e := (float64(actual) - float64(estimated)) / float64(actual)
			eAbs := math.Abs(e)
			if eAbs > maxVals[i][j] {
				maxVals[i][j] = eAbs
			}
			if eAbs > maxAllVals[i][j] {
				maxAllVals[i][j] = eAbs
			}
			vals[i][j] += e
			allVals[i][j] += e
		}
	}
	valCount[category]++
	valCount[0]++
	return true
}

func estimate() {
	startTime := time.Now()
	db := ethdb.MustOpen("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	//db := ethdb.MustOpen("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	//db := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	defer db.Close()
	maxi := 20
	maxj := 50
	//maxi := 2
	//maxj := 10
	trace := false
	maxValMap := make(map[int][][]float64)
	maxAllVals := make([][]float64, maxi)
	for i := 1; i < maxi; i++ {
		maxAllVals[i] = make([]float64, maxj)
	}
	maxValMap[0] = maxAllVals
	valMap := make(map[int][][]float64)
	allVals := make([][]float64, maxi)
	for i := 1; i < maxi; i++ {
		allVals[i] = make([]float64, maxj)
	}
	valMap[0] = allVals
	valCount := make(map[int]int)

	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	deleted := make(map[common.Address]bool) // Deleted contracts
	numDeleted := 0
	var current *llrb.LLRB
	count := 0
	contractCount := 0
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HashedAccountsBucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			copy(addr[:], k[:20])
			del, ok := deleted[addr]
			if !ok {
				v, err := tx.GetOne(dbutils.HashedAccountsBucket, crypto.Keccak256(addr[:]))
				if err != nil {
					return err
				}
				del = v == nil
				deleted[addr] = del
				if del {
					numDeleted++
				}
			}
			if del {
				continue
			}
			if _, ok := itemsByAddress[addr]; !ok {
				if current != nil {
					seed, err := storageRoot(db.KV(), addr)
					check(err)
					//if contractCount == 4 {
					done := estimateContract(contractCount, current, seed, valMap, maxValMap, valCount, maxi, maxj, trace)
					if done && trace {
						return nil
					}
					//}
					contractCount++
					if contractCount%1000 == 0 {
						fmt.Printf("Processed contracts: %d\n", contractCount)
					}
				}
				current = llrb.New()
			}
			ki := &KeyItem{}
			copy(ki.key[:], k[20:])
			current.InsertNoReplace(ki)
			itemsByAddress[addr]++
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records, deleted contracts: %d\n", count, numDeleted)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}

	for category, vals := range valMap {
		for i := 1; i < maxi; i++ {
			for j := 1; j < maxj; j++ {
				vals[i][j] /= float64(valCount[category])
			}
		}
	}

	fmt.Printf("Generating images...\n")
	for category, vals := range valMap {
		var maxe float64
		var mine float64 = 100000000.0
		for i := 1; i < maxi; i++ {
			for j := 1; j < maxj; j++ {
				a := math.Abs(vals[i][j])
				if a > maxe {
					maxe = a
				}
				if a < mine {
					mine = a
				}
			}
		}
		if maxe > 1.0 {
			maxe = 1.0
		}
		if maxe == mine {
			maxe = mine + 1.0
		}
		// Initialize the graphic context on an RGBA image
		imageWidth := 2000
		imageHeight := 480
		dest := image.NewRGBA(image.Rect(0, 0, imageWidth, imageHeight))
		gc := draw2dimg.NewGraphicContext(dest)

		// Set some properties
		gc.SetFillColor(color.RGBA{0x44, 0xff, 0x44, 0xff})
		gc.SetStrokeColor(color.RGBA{0x44, 0x44, 0x44, 0xff})
		gc.SetLineWidth(1)
		cellWidth := float64(imageWidth) / float64(maxj+1)
		cellHeight := float64(imageHeight) / float64(maxi+1)
		for i := 1; i < maxi; i++ {
			fi := float64(i)
			gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono})
			gc.SetFillColor(image.Black)
			gc.SetFontSize(12)
			gc.FillStringAt(fmt.Sprintf("%d", i), 5, (fi+0.5)*cellHeight)
		}
		for j := 1; j < maxj; j++ {
			fj := float64(j)
			gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono})
			gc.SetFillColor(image.Black)
			gc.SetFontSize(12)
			gc.FillStringAt(fmt.Sprintf("%d", j), fj*cellWidth+5, 0.5*cellHeight)
		}
		for i := 1; i < maxi; i++ {
			for j := 1; j < maxj; j++ {
				e := vals[i][j]
				heat := math.Abs(e)
				if heat > 1.0 {
					heat = 1.0
				}
				heat = (heat - mine) / (maxe - mine)
				red, green, blue := getHeatMapColor(heat)
				txt := fmt.Sprintf("%.1f%%", e*100.0)
				fi := float64(i)
				fj := float64(j)
				gc.BeginPath() // Initialize a new path
				gc.MoveTo(fj*cellWidth, fi*cellHeight)
				gc.LineTo(fj*cellWidth, (fi+1)*cellHeight)
				gc.LineTo((fj+1)*cellWidth, (fi+1)*cellHeight)
				gc.LineTo((fj+1)*cellWidth, fi*cellHeight)
				gc.LineTo(fj*cellWidth, fi*cellHeight)
				gc.Close()
				gc.SetFillColor(color.RGBA{byte(255.0 * red), byte(255.0 * green), byte(255.0 * blue), 0xff})
				gc.FillStroke()
				gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono})
				gc.SetFillColor(image.Black)
				gc.SetFontSize(8)
				gc.FillStringAt(txt, fj*cellWidth+5, (fi+0.5)*cellHeight)
			}
		}
		// Save to file
		draw2dimg.SaveToPngFile(fmt.Sprintf("heat_%d.png", category), dest)
	}
	fmt.Printf("Estimation took %s\n", time.Since(startTime))
}
