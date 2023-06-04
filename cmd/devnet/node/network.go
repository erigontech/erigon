package node

import (
	"sync"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/log/v3"
)

type Network struct {
	DataDir            string
	Chain              string
	Logger             log.Logger
	BasePrivateApiAddr string
	BaseRPCAddr        string
	Nodes              []NetworkNode
	wg                 sync.WaitGroup
	peers              []string
}

// Start starts the process for two erigon nodes running on the dev chain
func (nw *Network) Start() {
	for i, node := range nw.Nodes {
		node.Start(nw, i)

		// get the enode of the node
		// - note this has the side effect of waiting for the node to start
		if enode, err := node.getEnode(); err != nil {
			nw.peers = append(nw.peers, enode)

			// TODO we need to call AddPeer to the nodes to make them aware of this one
			// the current model only works for a 2 node network
		}

	}

	quitOnSignal(&nw.wg)
}

func (nw *Network) Wait() {
	nw.wg.Wait()
}

func (nw *Network) Node(nodeNumber int) *Node {
	return nw.Nodes[nodeNumber].node()
}

// QuitOnSignal stops the node goroutines after all checks have been made on the devnet
func quitOnSignal(wg *sync.WaitGroup) {
	models.QuitNodeChan = make(chan bool)
	go func() {
		for <-models.QuitNodeChan {
			// TODO this assumes 2 nodes and it should be node.Stop()
			wg.Done()
			wg.Done()
		}
	}()
}
