package devnet

import (
	context "context"
	"math/big"
	"regexp"
	"sync"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type Devnet []*Network

type NetworkSelector interface {
	Test(ctx context.Context, network *Network) bool
}

type NetworkSelectorFunc func(ctx context.Context, network *Network) bool

func (f NetworkSelectorFunc) Test(ctx context.Context, network *Network) bool {
	return f(ctx, network)
}

func (d Devnet) Start(ctx *cli.Context, logger log.Logger) (Context, error) {
	var wg sync.WaitGroup

	errors := make(chan error, len(d))

	runCtx := WithDevnet(context.Background(), ctx, d, logger)

	for _, network := range d {
		wg.Add(1)

		go func(nw *Network) {
			defer wg.Done()
			errors <- nw.Start(runCtx)
		}(network)
	}

	wg.Wait()

	close(errors)

	for err := range errors {
		if err != nil {
			d.Stop()
			return devnetContext{context.Background()}, err
		}
	}

	return runCtx, nil
}

func (d Devnet) Stop() {
	var wg sync.WaitGroup

	for _, network := range d {
		wg.Add(1)

		go func(nw *Network) {
			defer wg.Done()
			nw.Stop()
		}(network)
	}

	wg.Wait()
}

func (d Devnet) Wait() {
	var wg sync.WaitGroup

	for _, network := range d {
		wg.Add(1)

		go func(nw *Network) {
			defer wg.Done()
			nw.Wait()
		}(network)
	}

	wg.Wait()
}

func (d Devnet) SelectNetwork(ctx context.Context, selector interface{}) *Network {
	switch selector := selector.(type) {
	case int:
		if selector < len(d) {
			return d[selector]
		}
	case string:
		if exp, err := regexp.Compile("^" + selector); err == nil {
			for _, network := range d {
				if exp.MatchString(network.Chain) {
					return network
				}
			}
		}
	case *big.Int:
		for _, network := range d {
			if network.ChainID().Cmp(selector) == 0 {
				return network
			}
		}
	case NetworkSelector:
		for _, network := range d {
			if selector.Test(ctx, network) {
				return network
			}
		}
	}

	return nil
}
