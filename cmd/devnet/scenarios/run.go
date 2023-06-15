package scenarios

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
)

type SimulationInitializer func(*SimulationContext)

func Run(ctx context.Context, scenarios ...*Scenario) error {
	return runner{scenarios: scenarios}.runWithOptions(ctx, getDefaultOptions())
}

type runner struct {
	randomize     bool
	stopOnFailure bool

	scenarios []*Scenario

	simulationInitializer SimulationInitializer
}

func (r *runner) concurrent(ctx context.Context, rate int) (err error) {
	var copyLock sync.Mutex

	queue := make(chan int, rate)
	scenarios := make([]*Scenario, len(r.scenarios))

	if r.randomize {
		for i := range r.scenarios {
			j := devnetutils.RandomInt(i + 1)
			scenarios[i] = r.scenarios[j]
		}

	} else {
		copy(scenarios, r.scenarios)
	}

	simulationContext := SimulationContext{
		suite: &suite{
			randomize:      r.randomize,
			defaultContext: ctx,
			stepRunners:    stepRunners(ctx),
		},
	}

	for i, s := range scenarios {
		scenario := *s

		queue <- i // reserve space in queue

		runScenario := func(err *error, Scenario *Scenario) {
			defer func() {
				<-queue // free a space in queue
			}()

			if r.stopOnFailure && *err != nil {
				return
			}

			// Copy base suite.
			suite := *simulationContext.suite

			if r.simulationInitializer != nil {
				sc := SimulationContext{suite: &suite}
				r.simulationInitializer(&sc)
			}

			_, serr := suite.runScenario(&scenario)
			if suite.shouldFail(serr) {
				copyLock.Lock()
				*err = serr
				copyLock.Unlock()
			}
		}

		if rate == 1 {
			// Running within the same goroutine for concurrency 1
			// to preserve original stacks and simplify debugging.
			runScenario(&err, &scenario)
		} else {
			go runScenario(&err, &scenario)
		}
	}

	// wait until last are processed
	for i := 0; i < rate; i++ {
		queue <- i
	}

	close(queue)

	return err
}

func (runner runner) runWithOptions(ctx context.Context, opt *Options) error {
	//var output io.Writer = os.Stdout
	//if nil != opt.Output {
	//	output = opt.Output
	//}

	if opt.Concurrency < 1 {
		opt.Concurrency = 1
	}

	return runner.concurrent(ctx, opt.Concurrency)
}

type Options struct {
	Concurrency int
}

func getDefaultOptions() *Options {
	opt := &Options{
		Concurrency: 1,
	}
	return opt
}
