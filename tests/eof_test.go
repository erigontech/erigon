package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/vm"
)

func TestEOFValidation(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	et := new(testMatcher)

	dir := filepath.Join(".", "eof_tests/osaka/eip7692_eof_v1")

	et.walk(t, dir, func(t *testing.T, name string, test *EOFTest) {
		// import pre accounts & construct test genesis block & state root
		if err := et.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
		fmt.Println("---------------------------------")
	})
}

func TestEOFBlockchain(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)

	dir := filepath.Join(".", "blockchain_tests/osaka/eip7692_eof_v1")

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, true)); err != nil {
			t.Error(err)
		}
		fmt.Println("---------------------------------")
	})
}

func TestEOFStateTest(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	st := new(testMatcher)

	dir := filepath.Join(".", "state_tests/osaka/eip7692_eof_v1")

	dirs := datadir.New(t.TempDir())
	db, _ := temporaltest.NewTestDB(t, dirs)
	st.walk(t, dir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			t.Run(key, func(t *testing.T) {
				withTrace(t, func(vmconfig vm.Config) error {
					tx, err := db.BeginRw(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					_, _, err = test.Run(tx, subtest, vmconfig, dirs)
					tx.Rollback()
					if err != nil && len(test.json.Post[subtest.Fork][subtest.Index].ExpectException) > 0 {
						// Ignore expected errors
						return nil
					}
					return st.checkFailure(t, err)
				})
			})
		}
	})
}

// func withTrace_(t *testing.T, test func(vm.Config) error) {
// 	// Use config from command line arguments.
// 	config := vm.Config{}
// 	err := test(config)
// 	if err == nil {
// 		return
// 	}

// 	// Test failed, re-run with tracing enabled.
// 	t.Error(err)
// 	buf := new(bytes.Buffer)
// 	w := bufio.NewWriter(buf)
// 	tracer := logger.NewJSONLogger(&logger.LogConfig{DisableMemory: true}, w)
// 	config.Debug, config.Tracer = true, tracer
// 	err2 := test(config)
// 	if !reflect.DeepEqual(err, err2) {
// 		t.Errorf("different error for second run: %v", err2)
// 	}
// 	w.Flush()
// 	if buf.Len() == 0 {
// 		t.Log("no EVM operation logs generated")
// 		//} else {
// 		//enable it if need extensive logging
// 		//t.Log("EVM operation log:\n" + buf.String())
// 	}
// 	//t.Logf("EVM output: 0x%x", tracer.Output())
// 	//t.Logf("EVM error: %v", tracer.Error())
// }
