package smt_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"gotest.tools/v3/assert"
)

func TestBatchDelete(t *testing.T) {
	keys := []utils.NodeKey{
		utils.NodeKey{10768458483543229763, 12393104588695647110, 7306859896719697582, 4178785141502415085},
		utils.NodeKey{7512520260500009967, 3751662918911081259, 9113133324668552163, 12072005766952080289},
		utils.NodeKey{4755722537892498409, 14621988746728905818, 15452350668109735064, 8819587610951133148},
		utils.NodeKey{6340777516277056037, 6264482673611175884, 1063722098746108599, 9062208133640346025},
		utils.NodeKey{6319287575763093444, 10809750365832475266, 6426706394050518186, 9463173325157812560},
		utils.NodeKey{15155415624738072211, 3736290188193138617, 8461047487943769832, 12188454615342744806},
		utils.NodeKey{15276670325385989216, 10944726794004460540, 9369946489424614125, 817372649097925902},
		utils.NodeKey{2562799672200229655, 18444468184514201072, 17883941549041529369, 407038781355273654},
		utils.NodeKey{10768458483543229763, 12393104588695647110, 7306859896719697582, 4178785141502415085},
		utils.NodeKey{7512520260500009967, 3751662918911081259, 9113133324668552163, 12072005766952080289},
		utils.NodeKey{4755722537892498409, 14621988746728905818, 15452350668109735064, 8819587610951133148},
	}

	valuesTemp := [][8]uint64{
		[8]uint64{0, 1, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 1, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 1, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{103184848, 115613322, 0, 0, 0, 0, 0, 0},
		[8]uint64{2, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{3038602192, 2317586098, 794977000, 2442751483, 2309555181, 2028447238, 1023640522, 2687173865},
		[8]uint64{3100, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
	}

	values := make([]utils.NodeValue8, 0)
	for _, vT := range valuesTemp {
		values = append(values, utils.NodeValue8{
			big.NewInt(0).SetUint64(vT[0]),
			big.NewInt(0).SetUint64(vT[1]),
			big.NewInt(0).SetUint64(vT[2]),
			big.NewInt(0).SetUint64(vT[3]),
			big.NewInt(0).SetUint64(vT[4]),
			big.NewInt(0).SetUint64(vT[5]),
			big.NewInt(0).SetUint64(vT[6]),
			big.NewInt(0).SetUint64(vT[7]),
		})
	}

	smtIncremental := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)
	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", false)
	for i, k := range keys {
		smtIncremental.Insert(k, values[i])
		_, err := smtBatch.InsertBatch(insertBatchCfg, []*utils.NodeKey{&k}, []*utils.NodeValue8{&values[i]}, nil, nil)
		assert.NilError(t, err)

		smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
		smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
		assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	}

	smtIncremental.DumpTree()
	fmt.Println()
	smtBatch.DumpTree()
	fmt.Println()

	assertSmtDbStructure(t, smtBatch, false)
}
