package main

import (
	"github.com/ledgerwatch/erigon/cmd/sentry/commands"
	"github.com/ledgerwatch/erigon/common/debug"
)

// generate the messages

func main() {
	defer debug.LogPanic()
	commands.Execute()
}
