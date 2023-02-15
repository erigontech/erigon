package main

import (
	"flag"
	"io/ioutil"
	"os"
	"path"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

var (
	testDir = flag.String("test-dir", "tests", "directory of consensus tests")
	//testNameFlag = flag.String("test-name", "", "name of test to run")
)

var supportedVersions = []string{"altair", "bellatrix"}

var testName, caseName string
var testVersion clparams.StateVersion

var passed, failed int

func stringToClVersion(s string) clparams.StateVersion {
	switch s {
	case "phase0":
		return clparams.Phase0Version
	case "altair":
		return clparams.AltairVersion
	case "bellatrix":
		return clparams.BellatrixVersion
	case "capella":
		return clparams.CapellaVersion
	}
	panic("u stink")
}
func iterateOverTests(dir, p string, depth int) {
	testDirLists, err := ioutil.ReadDir(p)
	if err != nil {
		log.Error("Could not read test dir", "dir", p, "err", err)
		return
	}
	for _, testType := range testDirLists {
		if depth == 1 {
			if !slices.Contains(supportedVersions, testType.Name()) {
				return
			}
			testVersion = stringToClVersion(testType.Name())
		}
		if depth == 3 {
			_, testName = path.Split(p)
			caseName = testType.Name()
		}

		if !testType.IsDir() {
			if ok, err := executeTest(p); err != nil {
				log.Warn("Test Failed", "err", err, "test", p)
				failed++
			} else {
				if ok {
					passed++
				}
			}
			return
		} else {
			iterateOverTests(testType.Name(), path.Join(p, testType.Name()), depth+1)
		}

	}
}
func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	flag.Parse()
	//path, _ := os.Getwd()
	os.Chdir(*testDir)
	iterateOverTests(*testDir, ".", 0)
	log.Info("Finished running tests", "passed", passed, "failed", failed)
	if failed > 0 {
		os.Exit(1)
	}
}
