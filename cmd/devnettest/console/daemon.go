package console

import (
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/rpcdaemon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
	"sync"
	"time"
)

func StartProcess(rpcFlags *utils.RPCFlags) {
	go erigon.RunNode()
	time.Sleep(2 * time.Second)
	go rpcdaemon.RunDaemon(rpcFlags)
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
