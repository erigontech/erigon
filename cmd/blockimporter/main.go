package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon/turbo/logging"
)

func main() {
	// Parse commandline arguments
	var (
		dbPath                  = flag.String("db", "./db", "database path")
		evmUrl                  = flag.String("evm", "http://127.0.0.1:8545", "EVM canister HTTP endpoint URL")
		secondaryBlockSourceUrl = flag.String("secondary-blocks-url", "", "URL of the secondary blocks source")
	)
	flag.Parse()

	logger := logging.GetLogger("blockimporter")
	settings := Settings{
		DBPath:        *dbPath,
		Logger:        logger,
		Terminated:    make(chan struct{}),
		RetryCount:    100,
		RetryInterval: time.Second,
		PollInterval:  time.Second,
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(settings.Terminated)
	}()

	blockSource := NewHttpBlockSource(*evmUrl)
	var secondaryBlockSource BlockSource
	if *secondaryBlockSourceUrl != "" {
		secondarySource := NewHttpBlockSource(*secondaryBlockSourceUrl)
		secondaryBlockSource = &secondarySource
	}
	err := RunImport(&settings, &blockSource, secondaryBlockSource)

	if err != nil {
		logger.Error(err.Error())
		panic(err)
	}
}
