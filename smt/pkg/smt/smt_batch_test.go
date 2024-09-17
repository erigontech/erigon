package smt_test

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
	"gotest.tools/v3/assert"
)

type BatchInsertDataHolder struct {
	acc             accounts.Account
	AddressAccount  libcommon.Address
	AddressContract libcommon.Address
	Bytecode        string
	Storage         map[string]string
}

func TestBatchSimpleInsert(t *testing.T) {
	keysRaw := []*big.Int{
		big.NewInt(8),
		big.NewInt(8),
		big.NewInt(1),
		big.NewInt(31),
		big.NewInt(31),
		big.NewInt(0),
		big.NewInt(8),
	}
	valuesRaw := []*big.Int{
		big.NewInt(17),
		big.NewInt(18),
		big.NewInt(19),
		big.NewInt(20),
		big.NewInt(0),
		big.NewInt(0),
		big.NewInt(0),
	}

	keyPointers := []*utils.NodeKey{}
	valuePointers := []*utils.NodeValue8{}

	smtIncremental := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)
	smtBatchNoSave := smt.NewSMT(nil, true)

	for i := range keysRaw {
		k := utils.ScalarToNodeKey(keysRaw[i])
		vArray := utils.ScalarToArrayBig(valuesRaw[i])
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		keyPointers = append(keyPointers, &k)
		valuePointers = append(valuePointers, v)

		smtIncremental.InsertKA(k, valuesRaw[i])
	}

	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", false)
	_, err := smtBatch.InsertBatch(insertBatchCfg, keyPointers, valuePointers, nil, nil)
	assert.NilError(t, err)

	_, err = smtBatchNoSave.InsertBatch(insertBatchCfg, keyPointers, valuePointers, nil, nil)
	assert.NilError(t, err)

	smtIncremental.DumpTree()
	fmt.Println()
	smtBatch.DumpTree()
	fmt.Println()
	fmt.Println()
	fmt.Println()

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	smtBatchNoSaveRootHash, _ := smtBatchNoSave.Db.GetLastRoot()
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtBatchNoSaveRootHash))

	assertSmtDbStructure(t, smtBatch, false)
}

func incrementalInsert(tree *smt.SMT, key, val []*big.Int) {
	for i := range key {
		k := utils.ScalarToNodeKey(key[i])
		tree.InsertKA(k, val[i])
	}
}

func batchInsert(tree *smt.SMT, key, val []*big.Int) {
	keyPointers := []*utils.NodeKey{}
	valuePointers := []*utils.NodeValue8{}

	for i := range key {
		k := utils.ScalarToNodeKey(key[i])
		vArray := utils.ScalarToArrayBig(val[i])
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		keyPointers = append(keyPointers, &k)
		valuePointers = append(valuePointers, v)
	}
	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", false)
	tree.InsertBatch(insertBatchCfg, keyPointers, valuePointers, nil, nil)
}

func BenchmarkIncrementalInsert(b *testing.B) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smtIncremental := smt.NewSMT(nil, false)
		incrementalInsert(smtIncremental, keys, vals)
	}
}

func BenchmarkBatchInsert(b *testing.B) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smtBatch := smt.NewSMT(nil, false)
		batchInsert(smtBatch, keys, vals)
	}
}

func BenchmarkBatchInsertNoSave(b *testing.B) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smtBatch := smt.NewSMT(nil, true)
		batchInsert(smtBatch, keys, vals)
	}
}

func TestBatchSimpleInsert2(t *testing.T) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	smtIncremental := smt.NewSMT(nil, false)
	incrementalInsert(smtIncremental, keys, vals)

	smtBatch := smt.NewSMT(nil, false)
	batchInsert(smtBatch, keys, vals)

	smtBatchNoSave := smt.NewSMT(nil, false)
	batchInsert(smtBatchNoSave, keys, vals)

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	smtBatchNoSaveRootHash, _ := smtBatchNoSave.Db.GetLastRoot()

	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtBatchNoSaveRootHash))
}

func TestBatchWitness(t *testing.T) {
	keys := []utils.NodeKey{
		utils.NodeKey{17822804428864912231, 4683868963463720294, 2947512351908939790, 2330225637707749973},
		utils.NodeKey{15928606457751385034, 926210564408807848, 3634217732472610234, 18021748560357139965},
		utils.NodeKey{1623861826376204094, 570263533561698889, 4654109133431364496, 7281957057362652730},
		utils.NodeKey{13644513224119225920, 15807577943241006501, 9942496498562648573, 15190659753926523377},
		utils.NodeKey{9275812266666786730, 4204572028245381139, 3605834086260069958, 10007478335141208804},
		utils.NodeKey{8235907590678154663, 6691762687086189695, 15487167600723075149, 10984821506434298343},
		utils.NodeKey{16417603439618455829, 5362127645905990998, 10661203900902368419, 16076124886006448905},
		utils.NodeKey{11707747219427568787, 933117036015558858, 16439357349021750126, 14064521656451211675},
		utils.NodeKey{10768458483543229763, 12393104588695647110, 7306859896719697582, 4178785141502415085},
		utils.NodeKey{7512520260500009967, 3751662918911081259, 9113133324668552163, 12072005766952080289},
		utils.NodeKey{9944065905482556519, 8594459084728876791, 17786637052462706859, 15521772847998069525},
		utils.NodeKey{5036431633232956882, 16658186702978753823, 2870215478624537606, 11907126160741124846},
		utils.NodeKey{17938814940856978076, 13147879352039549979, 1303554763666506875, 14953772317105337015},
		utils.NodeKey{17398863357602626404, 4841219907503399295, 2992012704517273588, 16471435007473943078},
		utils.NodeKey{4763654225644445738, 5354841943603308259, 16476366216865814029, 10492509060169249179},
		utils.NodeKey{3554925909441560661, 16583852156861238748, 15693104712527552035, 8799937559790156794},
		utils.NodeKey{9617343367546549815, 6562355304138083186, 4016039301486039807, 10864657160754550133},
		utils.NodeKey{17933907347870222658, 16190350511466382228, 13330881818854499962, 1410294862891786839},
		utils.NodeKey{17260204906255015513, 15380909239227623493, 8567606678138088594, 4899143890802672405},
		utils.NodeKey{12539511585850227228, 3973200204826286539, 8108069613182344498, 11385621942985713904},
		utils.NodeKey{5984161349947667925, 7514232801604484380, 16331057190188025237, 2178913139230121631},
		utils.NodeKey{1993407781442332939, 1513605408256072860, 9533711780544200094, 4407755968940168245},
		utils.NodeKey{10660689026092155967, 7772873226204509526, 940412750970337957, 11934396459574454979},
		utils.NodeKey{13517500090161376813, 3430655983873553997, 5375259408796912397, 1582918923617071297},
		utils.NodeKey{1530581473737529386, 12702896566116465736, 5914767264290477911, 17646414071976395527},
		utils.NodeKey{16058468518382574435, 17573595348125839734, 14299084025723850432, 9173086175977268459},
		utils.NodeKey{3492167051156683621, 5113280701490269535, 3519293511105800335, 4519124618482063071},
		utils.NodeKey{18174025977752953446, 170880634573707059, 1420648486923115869, 7650935848186468717},
		utils.NodeKey{16208859541132551432, 6618660032536205153, 10385910322459208315, 8083618043937979883},
		utils.NodeKey{18055381843795531980, 13462709273291510955, 680380512647919587, 11342529403284590651},
		utils.NodeKey{14208409806025064162, 3405833321788641051, 10002545051615441056, 3286956713137532874},
		utils.NodeKey{5680425176740212736, 8706205589048866541, 1439054882559309464, 17935966873927915285},
		utils.NodeKey{110533614413158858, 1569162572987050699, 17606018854685897411, 14063722484766563720},
		utils.NodeKey{11233753640608616570, 12359586935502800882, 9900310098552340970, 2424696158120948624},
		utils.NodeKey{17470957289258137535, 89496548814733839, 13431046055752824170, 4863600257776330164},
		utils.NodeKey{12096080439449907754, 3586504186348650027, 16024032131582461863, 3698791599656620348},
		utils.NodeKey{12011265607191854676, 16995709771660398040, 10097323095148987140, 5271835541457063617},
		utils.NodeKey{13774341565485367328, 12574592232097177017, 13203533943886016969, 15689605306663468445},
		utils.NodeKey{17673889518692219847, 6954332541823247394, 954524149166700463, 10005323665613190430},
		utils.NodeKey{3390665384912132081, 273113266583762518, 15391923996500582086, 16937300536792272468},
		utils.NodeKey{3282365570547600329, 2269401659256178523, 12133143125482037239, 9431318293795439322},
		utils.NodeKey{10308056630015396434, 9302651503878791339, 1753436441509383136, 12655301298828119054},
		utils.NodeKey{4866095004323601391, 7715812469294898395, 13448442241363136994, 12560331541471347748},
		utils.NodeKey{9555357893875481640, 14044231432423634485, 2076021859364793876, 2098251167883986095},
		utils.NodeKey{13166561572768359955, 8774399027495495913, 17115924986198600732, 14679213838814779978},
		utils.NodeKey{1830856192880052688, 16817835989594317540, 6792141515706996611, 13263912888227522233},
		utils.NodeKey{8580776493878106180, 13275268150083925070, 1298114825004489111, 6818033484593972896},
		utils.NodeKey{2562799672200229655, 18444468184514201072, 17883941549041529369, 4070387813552736545},
		utils.NodeKey{9268691730026813326, 11545055880246569979, 1187823334319829775, 17259421874098825958},
		utils.NodeKey{9994578653598857505, 13890799434279521010, 6971431511534499255, 9998397274436059169},
		utils.NodeKey{18287575540870662480, 11943532407729972209, 15340299232888708073, 10838674117466297196},
		utils.NodeKey{14761821088000158583, 964796443048506502, 5721781221240658401, 13211032425907534953},
		utils.NodeKey{18144880475727242601, 4972225809077124674, 14334455111087919063, 8111397810232896953},
		utils.NodeKey{16933784929062172058, 9574268379822183272, 4944644580885359493, 3289128208877342006},
		utils.NodeKey{8619895206600224966, 15003370087833528133, 8252241585179054714, 9201580897217580981},
		utils.NodeKey{16332458695522739594, 7936008380823170261, 1848556403564669799, 17993420240804923523},
		utils.NodeKey{6515233280772008301, 4313177990083710387, 4012549955023285042, 12696650320500651942},
		utils.NodeKey{6070193153822371132, 14833198544694594099, 8041604520195724295, 569408677969141468},
		utils.NodeKey{18121124933744588643, 14019823252026845797, 497098216249706813, 14507670067050817524},
		utils.NodeKey{10768458483543229763, 12393104588695647110, 7306859896719697582, 4178785141502415085},
		utils.NodeKey{7512520260500009967, 3751662918911081259, 9113133324668552163, 12072005766952080289},
		utils.NodeKey{5911840277575969690, 14631288768946722660, 9289463458792995190, 11361263549285604206},
		utils.NodeKey{5112807231234019664, 3952289862952962911, 12826043220050158925, 4455878876833215993},
		utils.NodeKey{16417603439618455829, 5362127645905990998, 10661203900902368419, 16076124886006448905},
		utils.NodeKey{11707747219427568787, 933117036015558858, 16439357349021750126, 14064521656451211675},
		utils.NodeKey{16208859541132551432, 6618660032536205153, 10385910322459208315, 8083618043937979883},
		utils.NodeKey{18055381843795531980, 13462709273291510955, 680380512647919587, 11342529403284590651},
		utils.NodeKey{2562799672200229655, 18444468184514201072, 17883941549041529369, 4070387813552736545},
		utils.NodeKey{16339509425341743973, 7562720126843377837, 6087776866015284100, 13287333209707648581},
		utils.NodeKey{1830856192880052688, 16817835989594317540, 6792141515706996611, 13263912888227522233},
	}

	valuesTemp := [][8]uint64{
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2802548736, 3113182143, 10842021, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{552894464, 46566, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{4, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{8, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{92883624, 129402807, 3239216982, 1921492768, 41803744, 3662741242, 922499619, 611206845},
		[8]uint64{2149, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1220686685, 2241513088, 3059933278, 877008478, 3450374550, 2577819195, 3646855908, 1714882695},
		[8]uint64{433, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1807748760, 2873297298, 945201229, 411604167, 1063664423, 1763702642, 2637524917, 1284041408},
		[8]uint64{2112, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2407450438, 2021315520, 3591671307, 1981785129, 893348094, 802675915, 3804752326, 2006944699},
		[8]uint64{2583, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1400751902, 190749285, 93436423, 2918498711, 3630577401, 3928294404, 1037307865, 2336717508},
		[8]uint64{10043, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2040622618, 1654767043, 2359080366, 3993652948, 2990917507, 41202511, 3266270425, 2537679611},
		[8]uint64{2971, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2958032465, 981708138, 2081777150, 750201226, 3046928486, 2765783602, 2851559840, 1406574120},
		[8]uint64{23683, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1741943335, 1540916232, 1327285029, 2450002482, 2695899944, 0, 0, 0},
		[8]uint64{3109587049, 2273239893, 220080300, 1823520391, 35937659, 0, 0, 0},
		[8]uint64{1677672755, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{337379899, 3225725520, 234013414, 1425864754, 2013026225, 0, 0, 0},
		[8]uint64{1031512883, 3743101878, 2828268606, 2468973124, 1081703471, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{256, 481884672, 2932392155, 111365737, 1511099657, 224351860, 164, 0},
		[8]uint64{632216695, 2300948800, 3904328458, 2148496278, 971473112, 0, 0, 0},
		[8]uint64{1031512883, 3743101878, 2828268606, 2468973124, 1081703471, 0, 0, 0},
		[8]uint64{4, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2401446452, 1128446136, 4183588423, 3903755242, 16083787, 848717237, 2276372267, 2020002041},
		[8]uint64{2793696421, 3373683791, 3597304417, 3609426094, 2371386802, 1021540367, 828590482, 1599660962},
		[8]uint64{2793696421, 3373683791, 3597304417, 3609426094, 2371386802, 1021540367, 828590482, 1599660962},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2793696421, 3373683791, 3597304417, 3609426094, 2371386802, 1021540367, 828590482, 1599660962},
		[8]uint64{2793696421, 3373683791, 3597304417, 3609426094, 2371386802, 1021540367, 828590482, 1599660962},
		[8]uint64{86400, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{0, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1321730048, 465661287, 0, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{1480818688, 2647520856, 10842021, 0, 0, 0, 0, 0},
		[8]uint64{1, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2407450438, 2021315520, 3591671307, 1981785129, 893348094, 802675915, 3804752326, 2006944699},
		[8]uint64{2583, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{2, 0, 0, 0, 0, 0, 0, 0},
		[8]uint64{4210873971, 1869123984, 4035019538, 1823911763, 1097145772, 827956438, 819220988, 1111695650},
		[8]uint64{20, 0, 0, 0, 0, 0, 0, 0},
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

func TestBatchRawInsert(t *testing.T) {
	keysForBatch := []*utils.NodeKey{}
	valuesForBatch := []*utils.NodeValue8{}

	keysForIncremental := []utils.NodeKey{}
	valuesForIncremental := []utils.NodeValue8{}

	smtIncremental := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)

	rand.Seed(1)
	size := 1 << 10
	for i := 0; i < size; i++ {
		rawKey := big.NewInt(rand.Int63())
		rawValue := big.NewInt(rand.Int63())

		k := utils.ScalarToNodeKey(rawKey)
		vArray := utils.ScalarToArrayBig(rawValue)
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		keysForBatch = append(keysForBatch, &k)
		valuesForBatch = append(valuesForBatch, v)

		keysForIncremental = append(keysForIncremental, k)
		valuesForIncremental = append(valuesForIncremental, *v)

	}

	startTime := time.Now()
	for i := range keysForIncremental {
		smtIncremental.Insert(keysForIncremental[i], valuesForIncremental[i])
	}
	t.Logf("Incremental insert %d values in %v\n", len(keysForIncremental), time.Since(startTime))

	startTime = time.Now()

	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", true)
	_, err := smtBatch.InsertBatch(insertBatchCfg, keysForBatch, valuesForBatch, nil, nil)
	assert.NilError(t, err)
	t.Logf("Batch insert %d values in %v\n", len(keysForBatch), time.Since(startTime))

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))

	assertSmtDbStructure(t, smtBatch, false)

	// DELETE
	keysForBatchDelete := []*utils.NodeKey{}
	valuesForBatchDelete := []*utils.NodeValue8{}

	keysForIncrementalDelete := []utils.NodeKey{}
	valuesForIncrementalDelete := []utils.NodeValue8{}

	sizeToDelete := 1 << 14
	for i := 0; i < sizeToDelete; i++ {
		rawValue := big.NewInt(0)
		vArray := utils.ScalarToArrayBig(rawValue)
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		deleteIndex := rand.Intn(size)

		keyForBatchDelete := keysForBatch[deleteIndex]
		keyForIncrementalDelete := keysForIncremental[deleteIndex]

		keysForBatchDelete = append(keysForBatchDelete, keyForBatchDelete)
		valuesForBatchDelete = append(valuesForBatchDelete, v)

		keysForIncrementalDelete = append(keysForIncrementalDelete, keyForIncrementalDelete)
		valuesForIncrementalDelete = append(valuesForIncrementalDelete, *v)
	}

	startTime = time.Now()
	for i := range keysForIncrementalDelete {
		smtIncremental.Insert(keysForIncrementalDelete[i], valuesForIncrementalDelete[i])
	}
	t.Logf("Incremental delete %d values in %v\n", len(keysForIncrementalDelete), time.Since(startTime))

	startTime = time.Now()

	_, err = smtBatch.InsertBatch(insertBatchCfg, keysForBatchDelete, valuesForBatchDelete, nil, nil)
	assert.NilError(t, err)
	t.Logf("Batch delete %d values in %v\n", len(keysForBatchDelete), time.Since(startTime))

	assertSmtDbStructure(t, smtBatch, false)
}

func TestCompareAllTreesInsertTimesAndFinalHashesUsingDiskDb(t *testing.T) {
	incrementalDbPath := "/tmp/smt-incremental"
	smtIncrementalDb, smtIncrementalTx, smtIncrementalSmtDb := initDb(t, incrementalDbPath)

	bulkDbPath := "/tmp/smt-bulk"
	smtBulkDb, smtBulkTx, smtBulkSmtDb := initDb(t, bulkDbPath)

	batchDbPath := "/tmp/smt-batch"
	smtBatchDb, smtBatchTx, smtBatchSmtDb := initDb(t, batchDbPath)

	smtIncremental := smt.NewSMT(smtIncrementalSmtDb, false)
	smtBulk := smt.NewSMT(smtBulkSmtDb, false)
	smtBatch := smt.NewSMT(smtBatchSmtDb, false)

	compareAllTreesInsertTimesAndFinalHashes(t, smtIncremental, smtBulk, smtBatch)

	smtIncrementalTx.Commit()
	smtBulkTx.Commit()
	smtBatchTx.Commit()
	t.Cleanup(func() {
		smtIncrementalDb.Close()
		smtBulkDb.Close()
		smtBatchDb.Close()
		os.RemoveAll(incrementalDbPath)
		os.RemoveAll(bulkDbPath)
		os.RemoveAll(batchDbPath)
	})
}

func TestCompareAllTreesInsertTimesAndFinalHashesUsingInMemoryDb(t *testing.T) {
	smtIncremental := smt.NewSMT(nil, false)
	smtBulk := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)

	compareAllTreesInsertTimesAndFinalHashes(t, smtIncremental, smtBulk, smtBatch)
}

func compareAllTreesInsertTimesAndFinalHashes(t *testing.T, smtIncremental, smtBulk, smtBatch *smt.SMT) {
	batchInsertDataHolders, totalInserts := prepareData()
	ctx := context.Background()
	var incrementalError error

	accChanges := make(map[libcommon.Address]*accounts.Account)
	codeChanges := make(map[libcommon.Address]string)
	storageChanges := make(map[libcommon.Address]map[string]string)

	for _, batchInsertDataHolder := range batchInsertDataHolders {
		accChanges[batchInsertDataHolder.AddressAccount] = &batchInsertDataHolder.acc
		codeChanges[batchInsertDataHolder.AddressContract] = batchInsertDataHolder.Bytecode
		storageChanges[batchInsertDataHolder.AddressContract] = batchInsertDataHolder.Storage
	}

	startTime := time.Now()
	for addr, acc := range accChanges {
		if err := smtIncremental.SetAccountStorage(addr, acc); err != nil {
			incrementalError = err
		}
	}

	for addr, code := range codeChanges {
		if err := smtIncremental.SetContractBytecode(addr.String(), code); err != nil {
			incrementalError = err
		}
	}

	for addr, storage := range storageChanges {
		if _, err := smtIncremental.SetContractStorage(addr.String(), storage, nil); err != nil {
			incrementalError = err
		}
	}

	assert.NilError(t, incrementalError)
	t.Logf("Incremental insert %d values in %v\n", totalInserts, time.Since(startTime))

	startTime = time.Now()
	keyPointers, valuePointers, err := smtBatch.SetStorage(ctx, "", accChanges, codeChanges, storageChanges)
	assert.NilError(t, err)
	t.Logf("Batch insert %d values in %v\n", totalInserts, time.Since(startTime))

	keys := []utils.NodeKey{}
	for i, key := range keyPointers {
		v := valuePointers[i]
		if !v.IsZero() {
			smtBulk.Db.InsertAccountValue(*key, *v)
			keys = append(keys, *key)
		}
	}
	startTime = time.Now()
	smtBulk.GenerateFromKVBulk(ctx, "", keys)
	t.Logf("Bulk insert %d values in %v\n", totalInserts, time.Since(startTime))

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	smtBulkRootHash, _ := smtBulk.Db.GetLastRoot()
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	assert.Equal(t, utils.ConvertBigIntToHex(smtBulkRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))

	assertSmtDbStructure(t, smtBatch, true)
}

func initDb(t *testing.T, dbPath string) (kv.RwDB, kv.RwTx, *db.EriDb) {
	ctx := context.Background()

	os.RemoveAll(dbPath)

	dbOpts := mdbx.NewMDBX(log.Root()).Path(dbPath).Label(kv.ChainDB).GrowthStep(16 * datasize.MB).RoTxsLimiter(semaphore.NewWeighted(128))
	database, err := dbOpts.Open(ctx)
	if err != nil {
		t.Fatalf("Cannot create db %e", err)
	}

	migrator := migrations.NewMigrator(kv.ChainDB)
	if err := migrator.VerifyVersion(database); err != nil {
		t.Fatalf("Cannot verify db version %e", err)
	}
	// if err = migrator.Apply(database, dbPath); err != nil {
	// 	t.Fatalf("Cannot migrate db %e", err)
	// }

	// if err := database.Update(context.Background(), func(tx kv.RwTx) (err error) {
	// 	return params.SetErigonVersion(tx, "test")
	// }); err != nil {
	// 	t.Fatalf("Cannot update db")
	// }

	dbTransaction, err := database.BeginRw(ctx)
	if err != nil {
		t.Fatalf("Cannot craete db transaction")
	}

	db.CreateEriDbBuckets(dbTransaction)
	return database, dbTransaction, db.NewEriDb(dbTransaction)
}

func prepareData() ([]*BatchInsertDataHolder, int) {
	treeSize := 150
	storageSize := 96
	batchInsertDataHolders := make([]*BatchInsertDataHolder, 0)
	rand.Seed(1)
	for i := 0; i < treeSize; i++ {
		storage := make(map[string]string)
		addressAccountBytes := make([]byte, 20)
		addressContractBytes := make([]byte, 20)
		storageKeyBytes := make([]byte, 20)
		storageValueBytes := make([]byte, 20)
		rand.Read(addressAccountBytes)
		rand.Read(addressContractBytes)

		for j := 0; j < storageSize; j++ {
			rand.Read(storageKeyBytes)
			rand.Read(storageValueBytes)
			storage[libcommon.BytesToAddress(storageKeyBytes).Hex()] = libcommon.BytesToAddress(storageValueBytes).Hex()
		}

		acc := accounts.NewAccount()
		acc.Balance = *uint256.NewInt(rand.Uint64())
		acc.Nonce = rand.Uint64()

		batchInsertDataHolders = append(batchInsertDataHolders, &BatchInsertDataHolder{
			acc:             acc,
			AddressAccount:  libcommon.BytesToAddress(addressAccountBytes),
			AddressContract: libcommon.BytesToAddress(addressContractBytes),
			Bytecode:        "0x60806040526004361061007b5760003560e01c80639623609d1161004e5780639623609d1461012b57806399a88ec41461013e578063f2fde38b1461015e578063f3b7dead1461017e57600080fd5b8063204e1c7a14610080578063715018a6146100c95780637eff275e146100e05780638da5cb5b14610100575b600080fd5b34801561008c57600080fd5b506100a061009b366004610608565b61019e565b60405173ffffffffffffffffffffffffffffffffffffffff909116815260200160405180910390f35b3480156100d557600080fd5b506100de610255565b005b3480156100ec57600080fd5b506100de6100fb36600461062c565b610269565b34801561010c57600080fd5b5060005473ffffffffffffffffffffffffffffffffffffffff166100a0565b6100de610139366004610694565b6102f7565b34801561014a57600080fd5b506100de61015936600461062c565b61038c565b34801561016a57600080fd5b506100de610179366004610608565b6103e8565b34801561018a57600080fd5b506100a0610199366004610608565b6104a4565b60008060008373ffffffffffffffffffffffffffffffffffffffff166040516101ea907f5c60da1b00000000000000000000000000000000000000000000000000000000815260040190565b600060405180830381855afa9150503d8060008114610225576040519150601f19603f3d011682016040523d82523d6000602084013e61022a565b606091505b50915091508161023957600080fd5b8080602001905181019061024d9190610788565b949350505050565b61025d6104f0565b6102676000610571565b565b6102716104f0565b6040517f8f28397000000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8281166004830152831690638f283970906024015b600060405180830381600087803b1580156102db57600080fd5b505af11580156102ef573d6000803e3d6000fd5b505050505050565b6102ff6104f0565b6040517f4f1ef28600000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff841690634f1ef28690349061035590869086906004016107a5565b6000604051808303818588803b15801561036e57600080fd5b505af1158015610382573d6000803e3d6000fd5b5050505050505050565b6103946104f0565b6040517f3659cfe600000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8281166004830152831690633659cfe6906024016102c1565b6103f06104f0565b73ffffffffffffffffffffffffffffffffffffffff8116610498576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152602660248201527f4f776e61626c653a206e6577206f776e657220697320746865207a65726f206160448201527f646472657373000000000000000000000000000000000000000000000000000060648201526084015b60405180910390fd5b6104a181610571565b50565b60008060008373ffffffffffffffffffffffffffffffffffffffff166040516101ea907ff851a44000000000000000000000000000000000000000000000000000000000815260040190565b60005473ffffffffffffffffffffffffffffffffffffffff163314610267576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820181905260248201527f4f776e61626c653a2063616c6c6572206973206e6f7420746865206f776e6572604482015260640161048f565b6000805473ffffffffffffffffffffffffffffffffffffffff8381167fffffffffffffffffffffffff0000000000000000000000000000000000000000831681178455604051919092169283917f8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e09190a35050565b73ffffffffffffffffffffffffffffffffffffffff811681146104a157600080fd5b60006020828403121561061a57600080fd5b8135610625816105e6565b9392505050565b6000806040838503121561063f57600080fd5b823561064a816105e6565b9150602083013561065a816105e6565b809150509250929050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b6000806000606084860312156106a957600080fd5b83356106b4816105e6565b925060208401356106c4816105e6565b9150604084013567ffffffffffffffff808211156106e157600080fd5b818601915086601f8301126106f557600080fd5b81358181111561070757610707610665565b604051601f82017fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0908116603f0116810190838211818310171561074d5761074d610665565b8160405282815289602084870101111561076657600080fd5b8260208601602083013760006020848301015280955050505050509250925092565b60006020828403121561079a57600080fd5b8151610625816105e6565b73ffffffffffffffffffffffffffffffffffffffff8316815260006020604081840152835180604085015260005b818110156107ef578581018301518582016060015282016107d3565b5060006060828601015260607fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0601f83011685010192505050939250505056fea2646970667358221220372a0e10eebea1b7fa43ae4c976994e6ed01d85eedc3637b83f01d3f06be442064736f6c63430008110033",
			Storage:         storage,
		})
	}

	return batchInsertDataHolders, treeSize*4 + treeSize*storageSize
}

func assertSmtDbStructure(t *testing.T, s *smt.SMT, testMetadata bool) {
	smtBatchRootHash, _ := s.Db.GetLastRoot()

	actualDb, ok := s.Db.(*db.MemDb)
	if !ok {
		return
	}

	usedNodeHashesMap := make(map[string]*utils.NodeKey)
	assertSmtTreeDbStructure(t, s, utils.ScalarToRoot(smtBatchRootHash), usedNodeHashesMap)

	// EXPLAIN THE LINE BELOW: db could have more values because values' hashes are not deleted
	assert.Equal(t, true, len(actualDb.Db)-len(usedNodeHashesMap) >= 0)
	for k := range usedNodeHashesMap {
		_, found := actualDb.Db[k]
		assert.Equal(t, true, found)
	}

	totalLeaves := assertHashToKeyDbStrcture(t, s, utils.ScalarToRoot(smtBatchRootHash), testMetadata)
	assert.Equal(t, totalLeaves, len(actualDb.DbHashKey))
	if testMetadata {
		assert.Equal(t, totalLeaves, len(actualDb.DbKeySource))
	}

	assertTraverse(t, s)
}

func assertSmtTreeDbStructure(t *testing.T, s *smt.SMT, nodeHash utils.NodeKey, usedNodeHashesMap map[string]*utils.NodeKey) {
	if nodeHash.IsZero() {
		return
	}

	dbNodeValue, err := s.Db.Get(nodeHash)
	assert.NilError(t, err)

	nodeHashHex := utils.ConvertBigIntToHex(utils.ArrayToScalar(nodeHash[:]))
	usedNodeHashesMap[nodeHashHex] = &nodeHash

	if dbNodeValue.IsFinalNode() {
		nodeValueHash := utils.NodeKeyFromBigIntArray(dbNodeValue[4:8])
		dbNodeValue, err = s.Db.Get(nodeValueHash)
		assert.NilError(t, err)

		nodeHashHex := utils.ConvertBigIntToHex(utils.ArrayToScalar(nodeValueHash[:]))
		usedNodeHashesMap[nodeHashHex] = &nodeValueHash
		return
	}

	assertSmtTreeDbStructure(t, s, utils.NodeKeyFromBigIntArray(dbNodeValue[0:4]), usedNodeHashesMap)
	assertSmtTreeDbStructure(t, s, utils.NodeKeyFromBigIntArray(dbNodeValue[4:8]), usedNodeHashesMap)
}

func assertHashToKeyDbStrcture(t *testing.T, smtBatch *smt.SMT, nodeHash utils.NodeKey, testMetadata bool) int {
	if nodeHash.IsZero() {
		return 0
	}

	dbNodeValue, err := smtBatch.Db.Get(nodeHash)
	assert.NilError(t, err)

	if dbNodeValue.IsFinalNode() {
		memDb := smtBatch.Db.(*db.MemDb)

		nodeKey, err := smtBatch.Db.GetHashKey(nodeHash)
		assert.NilError(t, err)

		keyConc := utils.ArrayToScalar(nodeHash[:])
		k := utils.ConvertBigIntToHex(keyConc)
		_, found := memDb.DbHashKey[k]
		assert.Equal(t, found, true)

		if testMetadata {
			keyConc = utils.ArrayToScalar(nodeKey[:])

			_, found = memDb.DbKeySource[keyConc.String()]
			assert.Equal(t, found, true)
		}
		return 1
	}

	return assertHashToKeyDbStrcture(t, smtBatch, utils.NodeKeyFromBigIntArray(dbNodeValue[0:4]), testMetadata) + assertHashToKeyDbStrcture(t, smtBatch, utils.NodeKeyFromBigIntArray(dbNodeValue[4:8]), testMetadata)
}

func assertTraverse(t *testing.T, s *smt.SMT) {
	smtBatchRootHash, _ := s.Db.GetLastRoot()

	ctx := context.Background()
	action := func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
		if v.IsFinalNode() {
			valHash := v.Get4to8()
			v, err := s.Db.Get(*valHash)
			if err != nil {
				return false, err
			}

			if v[0] == nil {
				return false, fmt.Errorf("value is missing in the db")
			}

			vInBytes := utils.ArrayBigToScalar(utils.BigIntArrayFromNodeValue8(v.GetNodeValue8())).Bytes()
			if vInBytes == nil {
				return false, fmt.Errorf("error in converting to bytes")
			}

			return false, nil
		}

		return true, nil
	}
	err := s.Traverse(ctx, smtBatchRootHash, action)
	assert.NilError(t, err)
}
