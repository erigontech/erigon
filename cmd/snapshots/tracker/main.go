package main

import (
	"github.com/ledgerwatch/erigon/cmd/snapshots/tracker/commands"
	"github.com/ledgerwatch/erigon/common/debug"
)

func main() {
	defer debug.LogPanic()
	commands.Execute()
}
