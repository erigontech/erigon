package component

import (
	"context"
	"os"
	"runtime"
	"strconv"

	"github.com/erigontech/erigon-lib/app"
	"github.com/erigontech/erigon-lib/app/event"
	"github.com/erigontech/erigon-lib/app/util"
	"github.com/erigontech/erigon-lib/app/workerpool"
)

var rootComponentDomain *componentDomain

const POOL_LOAD_FACTOR = 4
const MIN_POOL_SIZE = 8

func init() {
	poolSize := runtime.NumCPU() * POOL_LOAD_FACTOR

	if envSize, err := strconv.Atoi(os.Getenv("COMPONENTS_EXEC_POOL_SIZE")); err == nil {
		poolSize = envSize
	}

	if poolSize < MIN_POOL_SIZE {
		poolSize = MIN_POOL_SIZE
	}

	execPool := workerpool.New(poolSize)
	rootComponentDomain = &componentDomain{nil, nil, execPool, nil}
	rootComponentDomain.sbus = event.NewServiceBus(rootComponentDomain)
	rootComponentDomain.Domain, _ = app.NewNamedDomain[string]("root",
		app.WithIdGenerator(app.PassThroughGenerator[string]()))
	component, _ := NewComponent[ComponentDomain](context.Background(),
		WithProvider(rootComponentDomain))

	rootComponentDomain.component = asComponent(component)
}

type ComponentDomain interface {
	Id() app.Id

	State() State
	AwaitState(ctx context.Context, state State) (State, error)

	Activate(ctx context.Context, handler ...ActivityHandler[ComponentDomain]) error
	Deactivate(ctx context.Context, handler ...ActivityHandler[ComponentDomain]) error

	serviceBus() *event.ServiceBus
}

type componentDomain struct {
	*component
	app.Domain
	execPool *workerpool.WorkerPool
	sbus     *event.ServiceBus
}

type serviceManager interface {
	ServiceBus() *event.ServiceBus
	Post(args ...interface{})
}

type domainOptions struct {
	dependent    *componentDomain
	execPoolSize *int
}

func WithDependentDomain(dependent ComponentDomain) app.Option {
	return app.WithOption[domainOptions](
		func(o *domainOptions) bool {
			o.dependent = dependent.(*componentDomain)
			return true
		})
}

func WithExecPoolSize(execPoolSize int) app.Option {
	return app.WithOption[domainOptions](
		func(o *domainOptions) bool {
			o.execPoolSize = &execPoolSize
			return true
		})
}

// NewComponentDomain creates a new component manager which will manage the lifecycle (activation and deactivation)
// of Components which are added as dependents.
//
// If the parent ComponentDomain is not nil its service bus will be used
// for despatching events, otherwise a new ServiceBus will be created to be used by this manager and its dependents.
//
// If an execPoolSize is passed as an argument a new worker pool will be created for the created ComponentDomain, otherwise
// it will use its parents workerpool, or if none is passed a process wide root pool.  In general it should only be
// necessary to create additional workerpools for situations where a lot of non-executable tasks are likely to be
// created - so that pool exhaustion does not lead to less than optimal cross process parrallelisation
func NewComponentDomain(context context.Context, id string, options ...app.Option) (ComponentDomain, error) {
	var cd *componentDomain
	var opts domainOptions

	options = app.ApplyOptions(&opts, options)

	if opts.dependent != nil {
		var execPool *workerpool.WorkerPool
		var poolSize int

		if opts.execPoolSize != nil {
			poolSize = *opts.execPoolSize
		} else {
			poolSize = int(float64(runtime.NumCPU()) * POOL_LOAD_FACTOR)

			if poolSize < MIN_POOL_SIZE {
				poolSize = MIN_POOL_SIZE
			}
		}

		execPool = workerpool.New(poolSize)

		cd = &componentDomain{nil, nil, execPool, nil}
	} else {
		if rootComponentDomain != nil {
			var execPool *workerpool.WorkerPool

			opts.dependent = rootComponentDomain

			if opts.execPoolSize != nil {
				execPool = workerpool.New(*opts.execPoolSize)
			}

			cd = &componentDomain{nil, nil, execPool, nil}

		} else {
			var poolSize int

			if *opts.execPoolSize > 0 {
				poolSize = *opts.execPoolSize
			} else {
				poolSize = int(float64(runtime.NumCPU()) * POOL_LOAD_FACTOR)

				if poolSize < MIN_POOL_SIZE {
					poolSize = MIN_POOL_SIZE
				}
			}

			execPool := workerpool.New(poolSize)
			cd = &componentDomain{nil, nil, execPool, nil}
		}
	}

	cd.Domain, _ = app.NewNamedDomain[string](id,
		app.WithIdGenerator(app.PassThroughGenerator[string]()))
	cd.sbus = event.NewServiceBus(cd)

	component, err := NewComponent[ComponentDomain](context,
		append(options,
			WithDependent(opts.dependent.component),
			WithProvider(cd),
			WithId(id))...)

	if err != nil {
		return nil, err
	}

	cd.component = asComponent(component)

	// this needs to happen after cd.component has been set
	var copts componentOptions
	app.ApplyOptions(&copts, options)

	for _, dependency := range copts.dependencies {
		cd.AddDependency(dependency)
	}

	return cd, nil
}

func (cd *componentDomain) Id() app.Id {
	return cd.component.Id()
}

func (cd *componentDomain) Activate(ctx context.Context, handler ...ActivityHandler[ComponentDomain]) error {
	return cd.activate(ctx, func(ctx context.Context, c *component, err error) {
		if len(handler) > 0 {
			handler[0].OnActivity(ctx, typedComponent[ComponentDomain]{cd.component}, c.State(), err)
		}
	})
}

func (cd *componentDomain) Deactivate(ctx context.Context, handler ...ActivityHandler[ComponentDomain]) error {
	return cd.component.deactivate(ctx, func(ctx context.Context, c *component, err error) {
		if len(handler) > 0 {
			handler[0].OnActivity(ctx, typedComponent[ComponentDomain]{cd.component}, c.State(), err)
		}
	})
}

// Exec executes a task in the mamagers workerpool.  This is primarily used for event processing
func (cd *componentDomain) Exec(task func()) {
	if cd.execPool != nil {
		cd.execPool.Submit(task)
	} else {
		if parentPool, ok := cd.component.Domain().(util.ExecPool); ok {
			parentPool.Exec(task)
		}
	}
}

func (cd *componentDomain) PoolSize() int {
	if cd.execPool != nil {
		return cd.execPool.Size()
	} else {
		if parentPool, ok := cd.component.Domain().(util.ExecPool); ok {
			return parentPool.PoolSize()
		}
	}

	return 0
}

func (cd *componentDomain) QueueSize() int {
	if cd.execPool != nil {
		return cd.execPool.WaitingQueueSize()
	} else {
		if parentPool, ok := cd.component.Domain().(util.ExecPool); ok {
			return parentPool.QueueSize()
		}
	}

	return 0
}

func (cd *componentDomain) deactivate() {
	if log.DebugEnabled() {
		log.Debug("Unregistering from Service Bus",
			"component", cd.Id().String())
	}

	if err := cd.serviceBus().UnregisterAll(cd); err != nil {
		if log.DebugEnabled() {
			log.Debug("Unregister from Service Bus failed",
				"component", cd.Id().String(),
				"err", err)
		}
	}

	if cd.sbus != nil {
		if log.DebugEnabled() {
			log.Debug("Deactivating Service Bus",
				"component", cd.Id().String())
		}
		cd.serviceBus().Deactivate()
	}

	if cd.execPool != nil {
		if log.DebugEnabled() {
			log.Debug("Stopping Exec Pool",
				"component", cd.Id().String())
		}
		cd.execPool.StopWait()
		cd.execPool = nil
	}
}

func (cd *componentDomain) serviceBus() *event.ServiceBus {
	if cd.sbus != nil {
		return cd.sbus
	}

	if parent, ok := cd.component.Domain().(serviceManager); ok {
		return parent.ServiceBus()
	}

	return nil
}
