package containers

import (

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/txpool"
)


// TxnRef stores the score and a reference to a TxnSlot object
type TxnRef struct {
	Score       int
	SenderAddr  *common.Address
	Slot        *txpool.TxnSlot
}