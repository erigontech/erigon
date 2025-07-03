package containers

import "sync"

type TxnBundle struct {
	Lock sync.RWMutex
	txns []TxnRef
	head uint64
	tail uint64
	maxCapacity uint64
	totalGas uint64
}


func(t *TxnBundle) Insert() {

}

func(t *TxnBundle) HasCapacity() {

}

