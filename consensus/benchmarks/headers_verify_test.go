package benchmarks

import (
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/clique"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

type result struct {
	diff    time.Duration
	percent float64
}

func TestVerifyHeadersEthash128(t *testing.T) {
	t.Skip("too slow")

	const toVerify = 129

	testVerifyHandlersEthash(t, toVerify)
}

func TestVerifyHeadersEthash256(t *testing.T) {
	t.Skip("too slow")

	const toVerify = 257

	testVerifyHandlersEthash(t, toVerify)
}

func TestVerifyHeadersEthash512(t *testing.T) {
	t.Skip("too slow")

	const toVerify = 513

	testVerifyHandlersEthash(t, toVerify)
}

func TestVerifyHeadersEthash1024(t *testing.T) {
	const toVerify = 1025

	testVerifyHandlersEthash(t, toVerify)
}

func TestVerifyHeadersEthash65536(t *testing.T) {
	const toVerify = 65536

	testVerifyHandlersEthash(t, toVerify)
}

func testVerifyHandlersEthash(t *testing.T, toVerify int) {
	hardTips, err := headerdownload.DecodeTips(ethashHardCodedHeaders[:toVerify])
	if err != nil {
		t.Fatal("while getting tips", err)
	}
	headers := toHeaders(hardTips)

	const threshold = 15 // percent
	const repeats = 5

	ethConfig := ethash.Config{
		CachesInMem:      1,
		CachesLockMmap:   false,
		DatasetDir:       "ethash",
		DatasetsInMem:    1,
		DatasetsOnDisk:   0,
		DatasetsLockMmap: false,
	}

	results := make([]result, repeats)
	resultsEthash := make([]time.Duration, repeats)

	for i := 0; i < repeats; i++ {
		resEthash := verifyByEngine(t, headers, core.DefaultGenesisBlock(), func(_ ethdb.Database) consensus.Engine {
			engine := ethash.New(ethConfig, nil, false)
			engine.SetThreads(-1)

			return engine
		})

		resultsEthash[i] = resEthash / time.Duration(len(headers))

		resEthashProcess := verifyByEngineProcess(t, headers, core.DefaultGenesisBlock(), &ethConfig)

		performanceDiff := float64(resEthash-resEthashProcess) / float64(resEthash) * 100

		results[i] = result{resEthash - resEthashProcess, performanceDiff}
	}

	checkResults(t, results, threshold)

	spew.Dump(resultsEthash)
}

func checkResults(t *testing.T, results []result, threshold float64) {
	t.Helper()

	msg := "results:\n"
	for _, r := range results {
		if r.percent < 0 && -r.percent > threshold {
			t.Errorf("diff is negative %v (%.2f%%)\n", r.diff, r.percent)
		}

		msg += fmt.Sprintf("%v\t%.2f%%\n", r.diff, r.percent)
	}

	t.Log(msg)
}

func TestVerifyHeadersClique128(t *testing.T) {
	t.Skip("too slow")

	const toVerify = 129

	testVerifyHeadersClique(t, toVerify)
}

func TestVerifyHeadersClique1024(t *testing.T) {
	const toVerify = 1025

	testVerifyHeadersClique(t, toVerify)
}

func TestVerifyHeadersClique65536(t *testing.T) {
	const toVerify = 65536

	testVerifyHeadersClique(t, toVerify)
}

func TestVerifyHeadersCliqueOnly1024(t *testing.T) {
	const toVerify = 1025

	testVerifyHeadersCliqueOnly(t, toVerify)
}

func TestVerifyHeadersCliqueOnly65536(t *testing.T) {
	t.Skip("too slow")
	const toVerify = 65536

	testVerifyHeadersCliqueOnly(t, toVerify)
}

func testVerifyHeadersClique(t *testing.T, toVerify int) {
	hardTips, err := headerdownload.DecodeTips(cliqueHardCodedHeaders[:toVerify])
	if err != nil {
		t.Fatal("while getting tips", err)
	}
	headers := toHeaders(hardTips)

	const threshold = 10 // percent
	const repeats = 10

	results := make([]result, repeats)

	for i := 0; i < repeats; i++ {
		resClique := verifyByEngine(t, headers, core.DefaultRinkebyGenesisBlock(), func(db ethdb.Database) consensus.Engine {
			return clique.New(params.RinkebyChainConfig.Clique, params.CliqueSnapshot, db)
		})

		resCliqueProcess := verifyByEngineProcess(t, headers, core.DefaultRinkebyGenesisBlock(), params.CliqueSnapshot)

		performanceDiff := float64(resClique-resCliqueProcess) / float64(resClique) * 100

		results[i] = result{resClique - resCliqueProcess, performanceDiff}
	}

	checkResults(t, results, threshold)
}

func testVerifyHeadersCliqueOnly(t *testing.T, toVerify int) {
	hardTips, err := headerdownload.DecodeTips(cliqueHardCodedHeaders[:toVerify])
	if err != nil {
		t.Fatal("while getting tips", err)
	}
	headers := toHeaders(hardTips)

	const repeats = 1

	var resCliqueProcess time.Duration

	for i := 0; i < repeats; i++ {
		resCliqueProcess = verifyByEngineProcess(t, headers, core.DefaultRinkebyGenesisBlock(), params.CliqueSnapshot)
	}

	fmt.Fprint(io.Discard, resCliqueProcess)
}

type engineConstructor func(db ethdb.Database) consensus.Engine

func verifyByEngine(t *testing.T, headers []*types.Header, genesis *core.Genesis, engineConstr engineConstructor) time.Duration {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	engine := engineConstr(db)
	defer engine.Close()

	config, _, err := core.SetupGenesisBlock(db, genesis, false, false)
	if err != nil {
		t.Errorf("setting up genensis block: %v", err)
	}

	chain, err := core.NewBlockChain(db, nil, config, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Error(err)
	}
	defer chain.Stop()

	seals := make([]bool, len(headers[1:]))
	for i := range seals {
		seals[i] = true
	}

	var done int

	step := 128
	from := 1

	var res time.Duration

loop:
	for {
		tn := time.Now()

		if done >= len(headers[1:]) {
			t.Logf("verifyByEngine done %d of %d\n", done, len(headers[1:]))
			break
		}

		to := from + step
		if to > len(headers) {
			to = len(headers)
		}

		cancel, results := engine.VerifyHeaders(chain, headers[from:to], seals)

		for err := range results {
			if err != nil {
				t.Errorf("while verifying %v, from %d to %d", err, from, to)
				cancel()
				break loop
			}
		}

		cancel()

		res += time.Since(tn)

		newCanonical, _, _, err := stagedsync.InsertHeaderChain("logPrefix", db, headers[from:to], time.Since(tn))
		if err != nil {
			t.Errorf("while inserting %v, from %d to %d", err, from, to)
			break
		}
		if !newCanonical {
			t.Errorf("a fork occurred from %d to %d", from, to)
			break
		}

		done += len(headers[from:to])
		from = done + 1
		step *= 2
	}

	return res
}

func verifyByEngineProcess(t *testing.T, headers []*types.Header, genesis *core.Genesis, consensusConfig interface{}) time.Duration {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	config, _, err := core.SetupGenesisBlock(db, genesis, false, false)
	if err != nil {
		t.Errorf("setting up genensis block: %v", err)
	}

	engine := ethconfig.CreateConsensusEngine(config, consensusConfig, nil, false, runtime.NumCPU())
	defer engine.Close()

	var done int
	var res time.Duration

	step := 128
	from := 1

	for {
		tn := time.Now()
		if done >= len(headers[1:]) {
			t.Logf("verifyByEngineProcess done %d of %d\n", done, len(headers[1:]))
			break
		}

		to := from + step
		if to > len(headers) {
			to = len(headers)
		}

		err = stagedsync.VerifyHeaders(db, headers[from:to], engine, 1)
		if err != nil {
			t.Error("error while VerifyHeaders",
				"from", headers[from].Number.Uint64(),
				"to", headers[to].Number.Uint64(),
				"error", err)
			break
		}

		res += time.Since(tn)

		newCanonical, _, _, err := stagedsync.InsertHeaderChain("logPrefix", db, headers[from:to], time.Since(tn))
		if err != nil {
			t.Errorf("while inserting %v, from %d to %d", err, from, to)
			break
		}
		if !newCanonical {
			t.Errorf("a fork occurred from %d to %d", from, to)
			break
		}

		done += len(headers[from:to])
		from = done + 1
		step *= 2
	}

	return res
}

func toHeaders(tips map[common.Hash]headerdownload.HeaderRecord) []*types.Header {
	headers := make([]*types.Header, 0, len(tips))
	for _, record := range tips {
		headers = append(headers, record.Header)
	}

	types.SortHeadersAsc(headers)

	return headers
}
