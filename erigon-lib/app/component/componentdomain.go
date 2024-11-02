package component

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/erigontech/erigon-lib/app"
	"github.com/erigontech/erigon-lib/app/event"
	"github.com/erigontech/erigon-lib/app/util"
	"github.com/erigontech/erigon-lib/app/workerpool"
	pkg_errors "github.com/pkg/errors"
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
	rootComponentDomain.serviceBus = event.NewServiceBus(rootComponentDomain)
	rootComponentDomain.Domain, _ = app.NewNamedDomain[string]("root",
		app.WithIdGenerator(app.PassThroughGenerator[string]()))
	rootId, _ := rootComponentDomain.NewId(context.Background(), "root")
	component, _ := NewComponent[ComponentDomain](context.Background(),
		WithProvider(rootComponentDomain),
		WithId(rootId))

	rootComponentDomain.component = asComponent(component)
}

type ComponentDomain interface {
	Id() app.Id
	ServiceBus() *event.ServiceBus
	Post(args ...interface{})
	Register(fns ...interface{}) (err error)
}

type componentDomain struct {
	*component
	app.Domain
	execPool   *workerpool.WorkerPool
	serviceBus *event.ServiceBus
}

type serviceManager interface {
	ServiceBus() *event.ServiceBus
	Post(args ...interface{})
}

type domainOptions struct {
	dependent    *componentDomain
	execPoolSize *int
}

func WithDependentDomain(dependent ComponentDomain) Option {
	return WithOption[domainOptions](
		func(o *domainOptions) {
			o.dependent = dependent.(*componentDomain)
		})
}

func WithExecPoolSize(execPoolSize int) Option {
	return WithOption[domainOptions](
		func(o *domainOptions) {
			o.execPoolSize = &execPoolSize
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
func NewComponentDomain(context context.Context, id string, options ...Option) (ComponentDomain, error) {
	var cd *componentDomain
	var opts domainOptions

	options = ApplyOptions(&opts, options)

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
	cd.serviceBus = event.NewServiceBus(cd)

	componentId, _ := cd.NewId(context, id)
	component, err := NewComponent[ComponentDomain](context,
		append(options,
			WithDependent(opts.dependent.component),
			WithProvider(cd),
			WithId(componentId))...)

	if err != nil {
		return nil, err
	}

	cd.component = asComponent(component)
	return cd, nil
}

func (cd *componentDomain) Id() app.Id {
	return cd.component.Id()
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

// lock to ensure that only one componentDomain collects and activates
// at a time - to avoid multiple activations of subsidiary components
var activationMutex = sync.Mutex{}

func (cd *componentDomain) Activate(activationContext context.Context) (chan *component, chan error) {
	cResOut := make(chan *component, 1)
	cErrOut := make(chan error, 1)

	activationMutex.Lock()
	cd.RLock()

	activationList := make([]*component, 0, len(cd.dependencies))
	var errors []error

	for _, dependency := range cd.dependencies {
		if !dependency.State().IsActivated() {
			if dependency.State() == Instantiated ||
				dependency.State() == Unknown {
				if configurable, ok := dependency.(Configurable); ok {
					if !dependency.State().IsConfigured() {
						err := func() (err error) {
							defer func() {
								if r := recover(); r != nil {
									var ok bool
									if err, ok = r.(error); ok {
										err = pkg_errors.WithStack(fmt.Errorf("%T configure panicked with error: %s", dependency, err))
									} else {
										err = pkg_errors.WithStack(fmt.Errorf("%T configure panicked: %v", dependency, r))
									}
								}
							}()

							return configurable.Configure(activationContext, asComponent(dependency).options...)
						}()

						if err != nil {
							errors = append(errors, err)
							continue
						}
					}
				}
			}

			activation := asComponent(dependency)
			activation.setState(Activating)
			activationList = append(activationList, activation)
		}

		if len(errors) > 0 {
			cErrOut <- fmt.Errorf("Activate failed with the following errors %v", errors)
			close(cResOut)
			close(cErrOut)
			cd.Unlock()
			return cResOut, cErrOut
		}
	}

	cd.RUnlock()
	activationMutex.Unlock()

	if cd.State().IsActivated() {
		if len(activationList) == 0 {
			cResOut <- cd.component
			close(cResOut)
			close(cErrOut)
		} else {
			cd.activateDependents(activationContext, activationList, cResOut, cErrOut)
		}
	} else {
		cd.setState(Activating)
		cd.activateDependents(activationContext, activationList, cResOut, cErrOut)
	}

	return cResOut, cErrOut
}

func (cd *componentDomain) activateDependents(activationContext context.Context, activationList []*component, cResOut chan *component, cErrOut chan error) {
	if len(activationList) == 0 {
		if log.Trace().Enabled() {
			log.Trace().
				Str("manager", util.LogInstance(cd)).
				Msg("Activated")
		}

		err := cd.onDependenciesActive(activationContext)

		if err != nil {
			cErrOut <- err
		} else {
			cResOut <- cd.component
		}

		close(cResOut)
		close(cErrOut)
	} else {
		var wg sync.WaitGroup
		dependentActivationContext, cancel := context.WithCancel(activationContext)
		errors := make([]error, 0, len(activationList))

		activate := func(dependency *component) {
			defer wg.Done()
			if log.Trace().Enabled() {
				log.Trace().
					Str("manager", util.LogInstance(cd)).
					Str("dependency", util.LogInstance(dependency)).
					Msg("Activating")
			}

			cres, cerr := dependency.activate(dependentActivationContext)

			if cres == nil || cerr == nil {
				errors = append(errors, fmt.Errorf("Activate Failed: Resolve channels are undefined"))
			}

			for {
				select {
				case _, ok := <-cres:
					if ok {
						if log.Trace().Enabled() {
							log.Trace().
								Str("manager", util.LogInstance(cd)).
								Str("dependency", util.LogInstance(dependency)).
								Msg("Dependency activated")
						}
						return
					}
				case err, ok := <-cerr:
					if ok {
						errors = append(errors, err)
						cancel()
						return
					}
				case <-dependentActivationContext.Done():
					return
				}
			}
		}

		wg.Add(len(activationList))
		for _, p := range activationList {
			go activate(p)
		}

		go func() {
			wg.Wait()
			if log.Trace().Enabled() {
				log.Trace().
					Str("manager", util.LogInstance(cd)).
					Msg("Activated")
			}
			if len(errors) > 0 {
				cErrOut <- fmt.Errorf("Activate failed with the following errors %v", errors)
			} else {
				err := cd.onDependenciesActive(activationContext)

				if err != nil {
					cErrOut <- err
				} else {
					cResOut <- cd.component
				}
			}
			close(cResOut)
			close(cErrOut)
		}()
	}
}

func (cd *componentDomain) Deactivate(deactivationContext context.Context) (chan *component, chan error) {
	cResOut, cErrOut := MakeResultChannels()

	if cd.State().IsDeactivated() {
		go func() {
			if _, err := cd.AwaitState(Deactivated); err != nil {
				cErrOut <- err
			} else {
				//fmt.Printf("%s Deactivated: %v\n", component.Name(), component.State())
				cResOut <- cd.component
			}
			close(cResOut)
			close(cErrOut)
		}()
		return cResOut, cErrOut
	}

	cd.setState(Deactivating)

	/*fmt.Printf("%s Deactivate Deps\n", manager.Name())
	for _, dep := range manager.dependents {
		fmt.Printf("  %v:%v:%p\n", manager.Name(), dep.Name(), dep)
	}*/

	deactivationList := make([]*component, len(cd.dependencies))

	cd.RLock()
	for i, component := range cd.dependencies {
		deactivationList[i] = asComponent(component)
	}
	cd.RUnlock()

	if len(deactivationList) == 0 {
		err := cd.onDependenciesDeactivated(deactivationContext)

		if err != nil {
			cErrOut <- err
		} else {
			cResOut <- cd.component
		}

		close(cResOut)
		close(cErrOut)
	} else {
		var wg sync.WaitGroup
		dependentDeactivationContext, cancel := context.WithCancel(deactivationContext)
		errors := make([]error, 0, len(deactivationList))

		deactivate := func(dependency *component) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						errors = append(errors, pkg_errors.WithStack(fmt.Errorf("%T Panicked with error: %w", dependency, err)))
					} else {
						errors = append(errors, pkg_errors.WithStack(fmt.Errorf("%T Panicked: %v", dependency, r)))
					}
				}
			}()

			//fmt.Printf("%s  Deactivating  %v:%p\n", manager.Name(), dependency.(Component).Name(), dependency)
			if dependency.State().IsDeactivated() {
				return
			}

			cres, cerr := dependency.deactivate(dependentDeactivationContext)

			if cres == nil || cerr == nil {
				errors = append(errors, fmt.Errorf("Deactivate Failed: Resolve channels are undefined"))
			}

			for {
				select {
				case _ /*component*/, ok := <-cres:
					if ok {
						//fmt.Printf("%s DEACTIVATED %s\n", manager.Name(), component.Name())
						return
					}
				case err, ok := <-cerr:
					if ok {
						errors = append(errors, err)
						cancel()
						return
					}
				case <-dependentDeactivationContext.Done():
					return
				}
			}
		}

		wg.Add(len(deactivationList))
		for _, p := range deactivationList {
			go deactivate(p)
		}

		go func() {
			wg.Wait()
			//fmt.Printf("Group Dectivation Done %v\n", manager)
			if len(errors) > 0 {
				cErrOut <- fmt.Errorf("Deactivate failed with the following errors %v", errors)
			} else {
				dependentsDeactivated := true

				for _, dependent := range deactivationList {
					if dependent.State() == Active {
						dependentsDeactivated = false
						break
					}
				}

				if dependentsDeactivated {
					err := cd.onDependenciesDeactivated(deactivationContext)

					if err != nil {
						cErrOut <- err
					} else {
						cResOut <- cd.component
					}
				} else {
					_, err := cd.AwaitState(Deactivated)

					if err != nil {
						cErrOut <- err
					} else {
						cResOut <- cd.component
					}
				}
			}
			close(cResOut)
			close(cErrOut)

			if log.Debug().Enabled() {
				log.Debug().
					Str("component", cd.Id().String()).
					Msg("Unregistering from Service Bus")
			}

			if err := cd.ServiceBus().UnregisterAll(cd); err != nil {
				if log.Debug().Enabled() {
					log.Debug().
						Str("component", cd.Id().String()).
						Err(err).
						Msg("Unregister from Service Bus failed")
				}
			}

			if cd.serviceBus != nil {
				if log.Debug().Enabled() {
					log.Debug().
						Str("component", cd.Id().String()).
						Msg("Deactivating Service Bus")
				}
				cd.ServiceBus().Deactivate()
			}

			if cd.execPool != nil {
				if log.Debug().Enabled() {
					log.Debug().
						Str("component", cd.Id().String()).
						Msg("Stopping Exec Pool")
				}
				cd.execPool.StopWait()
				cd.execPool = nil
			}
		}()
	}

	return cResOut, cErrOut
}

func (cd *componentDomain) ServiceBus() *event.ServiceBus {
	if cd.serviceBus != nil {
		return cd.serviceBus
	}

	if parent, ok := cd.component.Domain().(serviceManager); ok {
		return parent.ServiceBus()
	}

	return nil
}
