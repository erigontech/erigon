package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/erigontech/erigon/common/paths"
	"github.com/gateway-fm/zkevm-data-streamer/datastreamer"
	"github.com/gateway-fm/zkevm-data-streamer/log"
)

const (
	streamerSystemID    = 137
	streamerVersion     = 1
	streamTypeSequencer = 1
)

func main() {
	// Define command-line flags
	serverAddr := flag.String("server", "127.0.0.1:6900", "datastream server address to connect to")
	port := flag.Uint("port", 7900, "port to expose for clients to connect")
	dataFile := flag.String("datafile", filepath.Join(paths.DefaultDataDir(), "datarelay.bin"), "relay data file name")
	logLevel := flag.String("log", "info", "log level (debug, info, warn, error)")
	writeTimeoutMs := flag.Uint("writetimeout", 3000, "timeout for write operations on client connections in ms (0=no timeout)")
	inactivityTimeoutSec := flag.Uint("inactivitytimeout", 120, "timeout to kill an inactive client connection in seconds (0=no timeout)")

	flag.Parse()

	// Setup logging
	log.Init(log.Config{
		Environment: "development",
		Level:       *logLevel,
		Outputs:     []string{"stdout"},
	})

	writeTimeout := time.Duration(*writeTimeoutMs) * time.Millisecond
	inactivityTimeout := time.Duration(*inactivityTimeoutSec) * time.Second
	checkInterval := 5 * time.Second

	log.Infof(">> Relay server starting: port[%d] file[%s] server[%s] log[%s]",
		*port, *dataFile, *serverAddr, *logLevel)

	// Create relay server - directly use the implementation from gateway-fm/zkevm-data-streamer
	relay, err := datastreamer.NewRelay(
		*serverAddr,         // Server address
		uint16(*port),       // Port
		streamerVersion,     // Version
		streamerSystemID,    // System ID
		streamTypeSequencer, // Stream type
		*dataFile,           // Data file
		writeTimeout,        // Write timeout
		inactivityTimeout,   // Inactivity timeout
		checkInterval,       // Check interval
		nil,                 // No custom logger
	)

	if err != nil {
		log.Errorf(">> Relay server: NewRelay error! (%v)", err)
		os.Exit(1)
	}

	// Start relay server
	err = relay.Start()
	if err != nil {
		log.Errorf(">> Relay server: Start error! (%v)", err)
		os.Exit(1)
	}

	log.Infof(">> Relay server started successfully")

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for signal
	sig := <-sigChan
	log.Infof("Received signal %v, shutting down...", sig)

	// Stop the relay server gracefully
	if err := relay.Stop(); err != nil {
		log.Errorf(">> Error stopping relay server: %v", err)
		os.Exit(1)
	}
	log.Info(">> Relay server stopped")
}
