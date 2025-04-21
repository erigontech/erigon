package ethdb

import (
	"github.com/erigontech/erigon/turbo/ethdb/wasmdb"
	"github.com/erigontech/nitro-erigon/arbos/programs"
)

// InitializeLocalWasmTarget initializes the local WASM target based on the current arch.
func InitialiazeLocalWasmTarget() {
	lt := wasmdb.LocalTarget()
	desc := "description unavailable"
	switch lt {
	case wasmdb.TargetAmd64:
		desc = programs.DefaultTargetDescriptionX86
	case wasmdb.TargetArm64:
		desc = programs.DefaultTargetDescriptionArm
	}

	programs.SetTarget(lt, desc, true)
}
