package consensustests

import (
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

var supportedVersions = []string{"phase0", "altair", "bellatrix", "capella"}

type ConsensusTester struct {
	// parameters
	testDir string
	pattern *string // Pattern may or may not be present
	// metrics
	passed int
	failed int
	// internals
	context testContext
}

type testContext struct {
	testName string
	caseName string
	version  clparams.StateVersion
}

func New(testDir string, pattern *string) *ConsensusTester {
	return &ConsensusTester{
		testDir: testDir,
		pattern: pattern,
	}
}

// Run starts the tests runner.
func (c *ConsensusTester) Run() {
	os.Chdir(c.testDir)
	c.iterateOverTests(c.testDir, ".", 0)
}

// stringToClVersion converts the string to the current state version.
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

// iterateOverTests recursively iterate over the ethereum consensus tests.
func (c *ConsensusTester) iterateOverTests(dir, p string, depth int) {
	testDirLists, err := ioutil.ReadDir(p)
	if err != nil {
		log.Error("Could not read test dir", "dir", p, "err", err)
		return
	}
	for _, childDir := range testDirLists {
		childName := childDir.Name()
		// Depth 1 means that we are setting the version
		if depth == 1 {
			if !slices.Contains(supportedVersions, childName) {
				continue
			}
			c.context.version = stringToClVersion(childName)
		}
		// Depth 3 means that we are setting the whole test
		if depth == 3 {
			// depth 2 (equivalent) is pretty much the name of the test
			_, c.context.testName = path.Split(p)
			// depth 3 we find the specific
			c.context.caseName = childName
		}

		// If we found a non-directory then it is a test folder.
		if !childDir.IsDir() {
			// Check if it matches case specified.
			if *c.pattern != "" && !strings.Contains(p, *c.pattern) {
				return
			}
			log.Debug("Executing", "name", p)

			// If yes execute it.
			if implemented, err := c.executeTest(p); err != nil {
				log.Warn("Test Failed", "err", err, "test", p)
				c.failed++
			} else if implemented {
				// Mark it as passed only if the test was actually implemented had no errors were found.
				c.passed++
				log.Debug("Test passed", "name", p)
			}

			return
		} else {
			c.iterateOverTests(childName, path.Join(p, childName), depth+1)
		}

	}
}

// executeTest executes the test folder.
func (c *ConsensusTester) executeTest(p string) (bool, error) {
	// Silence all internal logging here
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlCrit, log.StderrHandler))
	initialPath, err := os.Getwd()
	if err != nil {
		return false, err
	}
	var implemented bool
	var fn testFunc
	// Change directory to test folder.
	os.Chdir(p)
	// If there is not a test handler skip it
	if fn, implemented = handlers[path.Join(c.context.testName, c.context.caseName)]; implemented {
		err = fn(c.context)
	}
	// Restore directory and folder
	os.Chdir(initialPath)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	return implemented, err
}

func (c *ConsensusTester) Metrics() (passed int, failed int) {
	return c.passed, c.failed
}
