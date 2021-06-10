package main

import (
	"github.com/ledgerwatch/erigon/cmd/sentry/commands"
)

var (
	// Following vars are injected through the build flags (see Makefile)
	gitCommit string
	gitBranch string
)

func main() {
	commands.GitCommit = gitCommit
	commands.GitCommit = gitBranch
	commands.Execute()
}
