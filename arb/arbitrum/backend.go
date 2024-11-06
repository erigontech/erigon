package arbitrum

import (
	"context"

	"github.com/erigontech/erigon/arb/arbitrum_types"
	"github.com/erigontech/erigon/arb/ethdb"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/bloombits"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/event"
	// "github.com/erigontech/erigon/internal/shutdowncheck"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/rpc"
)

type Backend struct {
	arb        ArbInterface
	stack      *node.Node
	apiBackend *APIBackend
	config     *Config
	chainDb    ethdb.Database

	txFeed event.Feed
	scope  event.SubscriptionScope

	bloomRequests chan chan *bloombits.Retrieval // Channel receiving bloom data retrieval requests
	bloomIndexer  *core.ChainIndexer             // Bloom indexer operating during block imports

	//shutdownTracker *shutdowncheck.ShutdownTracker

	chanTxs      chan *types.Transaction
	chanClose    chan struct{} //close coroutine
	chanNewBlock chan struct{} //create new L2 block unless empty

	filterSystem *filters.FilterSystem
}

func NewBackend(stack *node.Node, config *Config, chainDb ethdb.Database, publisher ArbInterface, filterConfig filters.Config) (*Backend, *filters.FilterSystem, error) {
	backend := &Backend{
		arb:     publisher,
		stack:   stack,
		config:  config,
		chainDb: chainDb,

		bloomRequests: make(chan chan *bloombits.Retrieval),
		bloomIndexer:  core.NewBloomIndexer(chainDb, config.BloomBitsBlocks, config.BloomConfirms),

		//shutdownTracker: shutdowncheck.NewShutdownTracker(chainDb),

		chanTxs:      make(chan *types.Transaction, 100),
		chanClose:    make(chan struct{}),
		chanNewBlock: make(chan struct{}, 1),
	}

	if len(config.AllowMethod) > 0 {
		rpcFilter := make(map[string]bool)
		for _, method := range config.AllowMethod {
			rpcFilter[method] = true
		}
		//backend.stack.ApplyAPIFilter(rpcFilter)
	}

	backend.bloomIndexer.Start(backend.arb.BlockChain())
	filterSystem, err := createRegisterAPIBackend(backend, filterConfig, config.ClassicRedirect, config.ClassicRedirectTimeout)
	if err != nil {
		return nil, nil, err
	}
	backend.filterSystem = filterSystem
	return backend, filterSystem, nil
}

// func (b *Backend) AccountManager() *accounts.Manager { return b.stack.AccountManager() }
func (b *Backend) APIBackend() *APIBackend          { return b.apiBackend }
func (b *Backend) APIs() []rpc.API                  { return b.apiBackend.GetAPIs(b.filterSystem) }
func (b *Backend) ArbInterface() ArbInterface       { return b.arb }
func (b *Backend) BlockChain() *core.BlockChain     { return b.arb.BlockChain() }
func (b *Backend) BloomIndexer() *core.ChainIndexer { return b.bloomIndexer }
func (b *Backend) ChainDb() ethdb.Database          { return b.chainDb }
func (b *Backend) Engine() consensus.Engine         { return b.arb.BlockChain().Engine() }
func (b *Backend) Stack() *node.Node                { return b.stack }

func (b *Backend) ResetWithGenesisBlock(gb *types.Block) {
	b.arb.BlockChain().ResetWithGenesisBlock(gb)
}

func (b *Backend) EnqueueL2Message(ctx context.Context, tx *types.Transaction, options *arbitrum_types.ConditionalOptions) error {
	return b.arb.PublishTransaction(ctx, tx, options)
}

func (b *Backend) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
	return b.scope.Track(b.txFeed.Subscribe(ch))
}

// TODO: this is used when registering backend as lifecycle in stack
func (b *Backend) Start() error {
	b.startBloomHandlers(b.config.BloomBitsBlocks)
	//b.shutdownTracker.MarkStartup()
	//b.shutdownTracker.Start()

	return nil
}

func (b *Backend) Stop() error {
	b.scope.Close()
	b.bloomIndexer.Close()
	//b.shutdownTracker.Stop()
	b.chainDb.Close()
	close(b.chanClose)
	return nil
}
