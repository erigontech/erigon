package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	log2 "github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
)

var (
	file                    = ""
	dataStreamServerFactory = server.NewZkEVMDataStreamServerFactory()
)

func main() {
	flag.StringVar(&file, "file", "", "datastream file")
	flag.Parse()

	logConfig := &log2.Config{
		Environment: "production",
		Level:       "info",
		Outputs:     []string{"stdout"},
	}

	stream, err := dataStreamServerFactory.CreateStreamServer(uint16(6900), uint8(3), 1, datastreamer.StreamType(1), file, 5*time.Second, 10*time.Second, 60*time.Second, logConfig)
	if err != nil {
		fmt.Println("Error creating datastream server:", err)
		return
	}

	go func() {
		err := stream.Start()
		if err != nil {
			fmt.Println("Error starting datastream server:", err)
			return
		}
	}()
	fmt.Println("Datastream server started")

	// listen for sigint to exit
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals

	fmt.Println("Shutting down datastream server")
}
