package ethdb

import (
	"fmt"

	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/nitro-erigon/arbos/programs"
)

//type WasmTarget string

func InitialiazeWasmTarget() {
	lt := state.LocalTarget()
	desc := "description unavailable"
	switch lt {
	case state.TargetAmd64:
		desc = programs.DefaultTargetDescriptionX86
	case state.TargetArm64:
		desc = programs.DefaultTargetDescriptionArm
	}

	fmt.Println("Initializing Wasm target:", lt, desc)
	programs.SetTarget(lt, desc, true)
}
