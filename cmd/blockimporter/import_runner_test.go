package main

import (
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/stretchr/testify/require"
)

type FileBasedBlockSource struct {
	files map[uint64]string
}

func NewFileBasedMockBlockSource() BlockSource {
	return FileBasedBlockSource{
		map[uint64]string{
			1: "./tests_data/blocks_1-3",
			4: "./tests_data/blocks_4",
			5: "./tests_data/blocks_5",
			6: "./tests_data/blocks_6",
		},
	}
}

func (blockSource FileBasedBlockSource) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	file, found := blockSource.files[fromBlock]
	if !found {
		return nil, nil
	}

	blocksData, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return readBlocksFromRlp(hex.NewDecoder(bytes.NewReader(blocksData)))
}

const ownerAccount string = "0xb0e5863d0ddf7e105e409fee0ecc0123a362e14b"

func (blockSource FileBasedBlockSource) GetInitialBalances() ([]BalanceEntry, error) {
	return []BalanceEntry{BalanceEntry{Address: common.HexToAddress(ownerAccount), Balance: *maxBalance()}}, nil
}

// Returns the max ETH balance to init a owner account
func maxBalance() *big.Int {
	var bytes [32]uint8
	for i := range bytes {
		bytes[i] = 255
	}

	val := &big.Int{}
	val.SetBytes(bytes[:])

	return val
}

func TestImport(t *testing.T) {
	logger := logging.GetLogger("blockimporter")
	settings := Settings{
		DBPath:        "./tmp_db",
		Logger:        logger,
		Terminated:    make(chan struct{}),
		RetryCount:    100,
		RetryInterval: time.Second,
		PollInterval:  time.Second,
	}
	os.RemoveAll(settings.DBPath)
	defer os.RemoveAll(settings.DBPath)

	go func() {
		time.Sleep(time.Second)
		close(settings.Terminated)
	}()
	err := RunImport(&settings, NewFileBasedMockBlockSource())
	require.Empty(t, err)
}
