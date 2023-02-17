package main

import (
	"os"
	"path"
)

func executeTest(p string) (bool, error) {

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
	return implemented, err
}
