package main

import (
	"os"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"

	"github.com/davecgh/go-spew/spew"
)

func main() {
	logger := newLogger()

	db, err := NewDB("./db", logger)
	if err != nil {
		panic(err)
	}

	state, err := NewState(db)
	if err != nil {
		panic(err)
	}

	for _, path := range os.Args[1:] {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}

		var block types.Block
		if err = block.DecodeRLP(rlp.NewStream(file, 0)); err != nil {
			panic(err)
		}

		spew.Dump(block)

		if err := state.ProcessBlock(block); err != nil {
			panic(err)
		}
	}
}

func newLogger() log.Logger {
	return logging.GetLogger("")
}
