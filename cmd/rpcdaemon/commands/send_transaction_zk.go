package commands

import (
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/zkchainconfig"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"math/big"
)

func (api *APIImpl) isZkNonSequencer(chainId *big.Int) bool {
	return !sequencer.IsSequencer() && zkchainconfig.IsZk(chainId.Uint64())
}

func (api *APIImpl) sendTxZk(rpcUrl string, encodedTx hexutility.Bytes, chainId uint64) (common.Hash, error) {
	res, err := client.JSONRPCCall(rpcUrl, "eth_sendRawTransaction", encodedTx)
	if err != nil {
		return common.Hash{}, err
	}

	if res.Error != nil {
		return common.Hash{}, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	//hash comes in escaped quotes, so we trim them here
	// \"0x1234\" -> 0x1234
	hashHex := strings.Trim(string(res.Result), "\"")

	return common.HexToHash(hashHex), nil
}
