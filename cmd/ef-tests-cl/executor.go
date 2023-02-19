package main

import (
	"os"
	"path"

	"github.com/ledgerwatch/log/v3"
)

func executeTest(p string) (bool, error) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlCrit, log.StderrHandler))
	initialPath, err := os.Getwd()
	if err != nil {
		return false, err
	}
	var implemented bool
	var fn testFunc
	os.Chdir(p)
	if fn, implemented = TestCollection[path.Join(testName, caseName)]; implemented {
		err = fn()
	}
	os.Chdir(initialPath)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	return implemented, err
}
