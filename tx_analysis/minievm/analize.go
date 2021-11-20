package minievm

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

type Analysis struct {
	ReadPoints  []common.Address
	WritePoints []common.Address
}

type Analizer struct {
	// evm

}

func Analize(header *types.Header, tx rpctest.EthTransaction) Analysis {

	analysis := Analysis{
		ReadPoints:  make([]common.Address, 0),
		WritePoints: make([]common.Address, 0),
	}

	if tx.To == nil {
		// contract creation
		fmt.Println("Contract creation")
	} else {
		// message call
		fmt.Println("Message call")
	}

	createEVMctx()

	return analysis
}

func createEVMctx() {

}

func create(sender common.Address, code []byte, value *uint256.Int) {

}
