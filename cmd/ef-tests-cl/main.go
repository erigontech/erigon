package main

import (
	"flag"
	"os"

	consensustests "github.com/ledgerwatch/erigon/cmd/ef-tests-cl/consensus_tests"
	"github.com/ledgerwatch/log/v3"
)

var (
	testDir      = flag.String("test-dir", "tests", "directory of consensus tests")
	testNameFlag = flag.String("case", "", "name of test to run")
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	tester := consensustests.New(*testDir, testNameFlag)
	flag.Parse()
	//path, _ := os.Getwd()

	tester.Run()
	passed, failed := tester.Metrics()
	log.Info("Finished running tests", "passed", passed, "failed", failed)
	if failed > 0 {
		os.Exit(1)
	}
}
