package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
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
		cpuprofile              = flag.String("cpuprofile", "", "write cpu profile to file")
		saveHistory             = flag.Bool("save-history-data", false, "save history data to the database")
	)
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	logger := logging.GetLogger("blockimporter")
	settings := Settings{
		DBPath:          *dbPath,
		Logger:          logger,
		Terminated:      make(chan struct{}),
		RetryCount:      100,
		RetryInterval:   time.Second,
		PollInterval:    time.Second,
		SaveHistoryData: *saveHistory,
	}

	c := make(chan os.Signal, 10)
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
