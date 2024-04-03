package main

import (
	"context"
	"encoding/binary"
	"fmt"
	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zkevm/etherman"
	"sort"
	"strings"
)

func formatBucketKVPair(k, v []byte, bucket string) string {
	// switch statement on bucket (found in tables.go)
	switch bucket {
	case kv.SyncStageProgress:
		val := binary.BigEndian.Uint64(v)
		return fmt.Sprintf("%s %d", string(k), val)

	case kv.Sequence:
		return fmt.Sprintf("%s %x", k, v)

	default:
		return fmt.Sprintf("%x %x", k, v)
	}
}

func printBuckets(chaindata, buckets string) {
	if buckets == "" {
		buckets = kv.EthTx
	}
	for _, b := range strings.Split(buckets, ",") {
		printBucket(chaindata, b)
	}
}

func countAccounts(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()

	var count uint64
	var keys []string

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.PlainState, nil, func(k, v []byte) error {
			if len(k) == 20 {
				count++
				keys = append(keys, common.Bytes2Hex(k))
			}
			return nil
		})
	}); err != nil {
		return err
	}

	fmt.Printf("count=%d\n", count)
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s\n", k)
	}

	return nil
}

func getOldAccInputHash(batchNum uint64) error {
	sig := "0x25280169" // hardcoded abi signature
	rollupID := "0000000000000000000000000000000000000000000000000000000000000001"
	batchNumber := fmt.Sprintf("%064x", batchNum)
	addr := libcommon.HexToAddress("0x9fB0B4A5d4d60aaCfa8DC20B8DF5528Ab26848d3")

	cfg := &ethconfig.Zk{
		L1ChainId: 11155111,
		L1RpcUrl:  "https://rpc.sepolia.org",
	}
	etherMan := newEtherMan(cfg)

	resp, err := etherMan.EthClient.CallContract(context.Background(), ethereum.CallMsg{
		To:   &addr,
		Data: common.FromHex(sig + rollupID + batchNumber),
	}, nil)

	if err != nil {
		return err
	}

	if len(resp) < 32 {
		return fmt.Errorf("response too short to contain hash data")
	}
	h := libcommon.BytesToHash(resp[:32])
	fmt.Printf("hash: %s\n", h.String())

	if len(resp) < 64 {
		return fmt.Errorf("response too short to contain timestamp data")
	}
	ts := binary.BigEndian.Uint64(resp[56:64])

	if len(resp) < 96 {
		return fmt.Errorf("response too short to contain last batch number data")
	}
	lastBatchNumber := binary.BigEndian.Uint64(resp[88:96])

	fmt.Println("timestamp: ", ts)
	fmt.Println("last batch number: ", lastBatchNumber)

	return nil
}

func newEtherMan(cfg *ethconfig.Zk) *etherman.Client {
	ethmanConf := etherman.Config{
		URL:                       cfg.L1RpcUrl,
		L1ChainID:                 cfg.L1ChainId,
		L2ChainID:                 cfg.L2ChainId,
		PoEAddr:                   cfg.AddressRollup,
		MaticAddr:                 cfg.L1MaticContractAddress,
		GlobalExitRootManagerAddr: cfg.AddressGerManager,
	}

	em, err := etherman.NewClient(ethmanConf)
	//panic on error
	if err != nil {
		panic(err)
	}
	return em
}
