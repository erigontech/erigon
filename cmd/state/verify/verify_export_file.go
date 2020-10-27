package verify

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func ExportFile(filePath, chaindataPath string) error {
	exportFile, err := stateless.NewBlockProviderFromExportFile(filePath)
	if err != nil {
		return err
	}

	createDb := func(path string) (*ethdb.ObjectDatabase, error) {
		return ethdb.Open(path, false)
	}

	chaindata, err := stateless.NewBlockProviderFromDB(chaindataPath, createDb)
	if err != nil {
		return err
	}

	fmt.Println("checking blocks...")

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		fmt.Println("\r\n- Ctrl+C pressed in Terminal, shutting down...")
		os.Exit(0)
	}()

	for {
		blockFromExport, err := exportFile.NextBlock()
		if err != nil {
			return err
		}

		if blockFromExport == nil { // no more blocks left
			fmt.Println("")
			fmt.Println("Export file verified OK")
			return nil // its okay
		}

		blockFromChaindata, err := chaindata.NextBlock()
		if err != nil {
			return err
		}

		if err := compareBlocks(blockFromExport, blockFromChaindata); err != nil {
			return err
		}

		fmt.Printf("\rVerified: blocks=%8d", blockFromExport.NumberU64())
	}
}

func compareBlocks(fromExport, fromChaindata *types.Block) error {
	if fromChaindata == nil {
		return fmt.Errorf("could not find block #%v in chaindata", fromExport.Number())
	}

	if fromExport.Number().Cmp(fromChaindata.Number()) != 0 {
		return fmt.Errorf("block numbers mismatch: export=%v chaindata=%v", fromExport.Number(), fromChaindata.Number())
	}

	fromExportHash := fromExport.Hash()
	fromChaindataHash := fromChaindata.Hash()
	if !bytes.Equal(fromExportHash[:], fromChaindataHash[:]) {
		// this error message intentionally is structured this way because it is printed to the console.
		//nolint:golint,stylecheck
		return fmt.Errorf("hashes mismatch:\n\t<    export=%x\n\t> chaindata=%x\n", fromExport.Hash(), fromChaindata.Hash())
	}

	return nil
}
