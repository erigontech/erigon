package main

import (
	"io"
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/restapi/commands"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

func main() {
	var (
		ostream log.Handler
		glogger *log.GlogHandler
	)

	usecolor := (isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
	output := io.Writer(os.Stderr)
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	ostream = log.StreamHandler(output, log.TerminalFormat(usecolor))
	glogger = log.NewGlogHandler(ostream)
	log.Root().SetHandler(glogger)
	glogger.Verbosity(log.LvlInfo)

	commands.Execute()
}
