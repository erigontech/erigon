package main

import (
	"flag"

	"github.com/ledgerwatch/erigon/metrics/exp"
	"github.com/ledgerwatch/erigon/turbo/debug"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cmd/caplin-regression/regression"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	_ "net/http/pprof" //nolint:gosec
)

var nameTestsMap = map[string]func(*forkchoice.ForkChoiceStore, *cltypes.SignedBeaconBlock) error{
	"TestRegressionWithValidation":    regression.TestRegressionWithValidation,
	"TestRegressionWithoutValidation": regression.TestRegressionWithoutValidation,
	"TestRegressionBadBlocks":         regression.TestRegressionBadBlocks,
}

var excludeTests = []string{"TestRegressionBadBlocks"}

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	test := flag.String("test", "TestRegressionWithValidation", "select test to run. can be TestRegressionWithValidation, TestRegressionWithoutValidation and TestRegressionBadBlocks")
	step := flag.Int("step", 32, "how often to log performance")
	pprof := flag.Bool("pprof", true, "turn on profiling")
	loop := flag.Bool("loop", true, "loop the test in an infinite loop")
	testsDir := flag.String("testsDir", "cmd/caplin-regression/caplin-tests", "directory to the tests")

	all := flag.Bool("all", true, "loop trhough all the test")

	flag.Parse()
	if _, ok := nameTestsMap[*test]; !ok {
		log.Error("Could not start regression tests", "err", "test not found")
		return
	}
	r, err := regression.NewRegressionTester(
		*testsDir,
	)
	if *pprof {
		// Server for pprof
		debug.StartPProf("localhost:6060", exp.Setup("localhost:6060", log.Root()))
	}

	if err != nil {
		log.Error("Could not start regression tests", "err", err)
		return
	}

	for val := true; val; val = *loop {
		if *all {
			for name, t := range nameTestsMap {
				if slices.Contains(excludeTests, name) {
					continue
				}
				if err := r.Run(name, t, *step); err != nil {
					log.Error("Could not do regression tests", "err", err)
				}
			}
			continue
		}
		if err := r.Run(*test, nameTestsMap[*test], *step); err != nil {
			log.Error("Could not do regression tests", "err", err)
		}
	}
}
