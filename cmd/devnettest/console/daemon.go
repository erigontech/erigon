package console

import (
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/rpcdaemon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
	"sync"
	"time"
)

func StartProcess(rpcFlags *utils.RPCFlags) {
	go rpcdaemon.RunDaemon(rpcFlags)
	time.Sleep(2 * time.Second)
	go erigon.RunNode()
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
