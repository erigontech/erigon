// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package scenarios

import (
	"context"
	"sync"

	"github.com/erigontech/erigon/cmd/devnet/devnetutils"
)

type SimulationInitializer func(*SimulationContext)

func Run(ctx context.Context, scenarios ...*Scenario) error {
	if len(scenarios) == 0 {
		return nil
	}

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
