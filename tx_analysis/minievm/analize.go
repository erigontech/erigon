package minievm

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
)

type Analysis struct {
	ReadPoints  []common.Address
	WritePoints []common.Address
}

type Analizer struct {
	// evm

}

func Analize(header *types.Header, tx rpctest.EthTransaction, sdb *state.IntraBlockState) (Analysis, error) {

	analysis := Analysis{
		ReadPoints:  make([]common.Address, 0),
		WritePoints: make([]common.Address, 0),
	}

	if tx.To == nil {
		// contract creation
		fmt.Println("Contract creation")
		// create()
	} else {
		// message call
		fmt.Println("Message call")
		// call()
	}

	createEVMctx()

	return analysis, nil
}

func createEVMctx() {

}

func create(sender common.Address, code []byte, value *uint256.Int) {

}

func create2() {

}

func call() {

}

func callCode() {

}

func delegateCall() {

}

func staticCall() {

}
