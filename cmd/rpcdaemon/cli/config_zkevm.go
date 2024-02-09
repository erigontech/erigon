package cli

import (
	"fmt"
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/log/v3"
)

func StartDataStream(server *datastreamer.StreamServer) error {
	if server == nil {
		// no stream server to start, we might not have the right flags set to create one
		return nil
	}

	log.Info("Starting data stream server...")
	err := server.Start()
	if err != nil {
		return fmt.Errorf("failed to start data stream server, error: %w", err)
	}

	return nil
}
