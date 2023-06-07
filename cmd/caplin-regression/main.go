package main

import (
	"flag"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cmd/caplin-regression/regression"
	"github.com/ledgerwatch/log/v3"

	_ "net/http/pprof" //nolint:gosec
)

var nameTestsMap = map[string]func(*forkchoice.ForkChoiceStore, *cltypes.SignedBeaconBlock) error{
	"TestRegressionWithValidation":    regression.TestRegressionWithValidation,
	"TestRegressionWithoutValidation": regression.TestRegressionWithoutValidation,
	"TestRegressionBadBlocks":         regression.TestRegressionBadBlocks,
}

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	test := flag.String("test", "TestRegressionWithValidation", "select test to run. can be TestRegressionWithValidation, TestRegressionWithoutValidation and TestRegressionBadBlocks")
	step := flag.Int("step", 1, "how often to log performance")
	pprof := flag.Bool("pprof", true, "turn on profiling")
	flag.Parse()
	if _, ok := nameTestsMap[*test]; !ok {
		log.Error("Could not start regression tests", "err", "test not found")
		return
	}
	r, err := regression.NewRegressionTester(
		"cmd/caplin-regression/caplin-tests",
	)
	if *pprof {
		// Server for pprof
		go func() {
			log.Info("Serving pprof on localhost:6060")
			if err := http.ListenAndServe("localhost:6060", nil); err != nil { //nolint:gosec
				log.Error("Could not serve pprof", "err", err)
			}

		}()
	}

	if err != nil {
		log.Error("Could not start regression tests", "err", err)
		return
	}

	if err := r.Run(*test, nameTestsMap[*test], *step); err != nil {
		log.Error("Could not do regression tests", "err", err)
	}
}
