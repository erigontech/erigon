package commands

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// splitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func splitAndTrim(input string) []string {
	result := strings.Split(input, ",")
	for i, r := range result {
		result[i] = strings.TrimSpace(r)
	}
	return result
}

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	remoteDbAdddress string
	db               *remote.DB
}

func (api *APIImpl) ensureConnected() error {
	if api.db == nil {
		conn, err := net.Dial("tcp", api.remoteDbAdddress)
		if err != nil {
			return err
		}
		api.db, err = remote.NewDB(conn, conn, conn)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectAPIImpl connects to the remote DB and returns APIImpl instance
func ConnectAPIImpl(remoteDbAdddress string) (*APIImpl, error) {
	return &APIImpl{remoteDbAdddress: remoteDbAdddress}, nil
}

// BlockNumber returns the currently highest block number available in the remote db
func (api *APIImpl) BlockNumber(ctx context.Context) (uint64, error) {
	if err := api.ensureConnected(); err != nil {
		return 0, err
	}
	var blockNumber uint64
	if err := api.db.View(func(tx *remote.Tx) error {
		b := tx.Bucket(dbutils.HeadHeaderKey)
		if b == nil {
			return fmt.Errorf("bucket %s not found", dbutils.HeadHeaderKey)
		}
		blockHashData := b.Get(dbutils.HeadHeaderKey)
		if len(blockHashData) != common.HashLength {
			return fmt.Errorf("head header hash not found or wrong size: %x", blockHashData)
		}
		b1 := tx.Bucket(dbutils.HeaderNumberPrefix)
		if b1 == nil {
			return fmt.Errorf("bucket %s not found", dbutils.HeaderNumberPrefix)
		}
		blockNumberData := b1.Get(blockHashData)
		if len(blockNumberData) != 8 {
			return fmt.Errorf("head block number not found or wrong size: %x", blockNumberData)
		}
		blockNumber = binary.BigEndian.Uint64(blockNumberData)
		return nil
	}); err != nil {
		api.db.Close()
		api.db = nil
		return 0, err
	}
	return blockNumber, nil
}

func daemon(cfg Config) {
	vhosts := splitAndTrim(cfg.rpcVirtualHost)
	cors := splitAndTrim(cfg.rpcCORSDomain)
	enabledApis := splitAndTrim(cfg.rpcAPI)
	var rpcAPI = []rpc.API{}
	apiImpl, err := ConnectAPIImpl(cfg.remoteDbAdddress)
	if err != nil {
		log.Error("Could not connect to remoteDb", "error", err)
		return
	}
	for _, enabledAPI := range enabledApis {
		switch enabledAPI {
		case "eth":
			var api EthAPI
			api = apiImpl
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   api,
				Version:   "1.0",
			})
		default:
			log.Error("Unrecognised", "api", enabledAPI)
		}
	}
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.rpcListenAddress, cfg.rpcPort)
	listener, _, err := rpc.StartHTTPEndpoint(httpEndpoint, rpcAPI, enabledApis, cors, vhosts, rpc.DefaultHTTPTimeouts)
	if err != nil {
		log.Error("Could not start RPC api", "error", err)
		return
	}
	extapiURL := fmt.Sprintf("http://%s", httpEndpoint)
	log.Info("HTTP endpoint opened", "url", extapiURL)

	defer func() {
		listener.Close()
		log.Info("HTTP endpoint closed", "url", httpEndpoint)
	}()

	abortChan := make(chan os.Signal)
	signal.Notify(abortChan, os.Interrupt)

	sig := <-abortChan
	log.Info("Exiting...", "signal", sig)
}
