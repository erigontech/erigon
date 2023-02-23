package main

import (
	"math/big"
	"os"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"

	"github.com/davecgh/go-spew/spew"
)

func main() {
	logger := newLogger()

	db, err := NewDB(".", logger)
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

func max_balance() *big.Int {
	var bytes [32]uint8
	for i, _ := range bytes {
		bytes[i] = 255
	}

	val := &big.Int{}
	val.SetBytes(bytes[:])

	return val
}

func newLogger() log.Logger {
	return logging.GetLogger("")
}
