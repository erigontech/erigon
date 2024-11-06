package rawdb

import (
	"github.com/erigontech/erigon/arb/ethdb"
	"runtime"
)

const (
	TargetWavm  ethdb.WasmTarget = "wavm"
	TargetArm64 ethdb.WasmTarget = "arm64"
	TargetAmd64 ethdb.WasmTarget = "amd64"
	TargetHost  ethdb.WasmTarget = "host"
)

func LocalTarget() ethdb.WasmTarget {
	if runtime.GOOS == "linux" {
		switch runtime.GOARCH {
		case "arm64":
			return TargetArm64
		case "amd64":
			return TargetAmd64
		}
	}
	return TargetHost
}

var CodePrefix = []byte("c") // CodePrefix + code hash -> account code
