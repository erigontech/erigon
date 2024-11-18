package component

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/erigontech/erigon-lib/app"
	"github.com/erigontech/erigon-lib/app/event"
	"github.com/erigontech/erigon-lib/app/util"
	liblog "github.com/erigontech/erigon-lib/log/v3"
	pkg_errors "github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

type relation interface {
	Id() app.Id
	Name() string

	State() State
	AwaitState(ctx context.Context, state State) (State, error)

	Context() context.Context

	Lock()
	Unlock()

	GetDependency(context context.Context, selector app.TypedSelector) relation
	AddDependency(component relation) relation
	RemoveDependency(component relation)

	Dependents() relations
	HasDependent(component relation) bool

	Domain() ComponentDomain
	Flags() []cli.Flag

	isConfigurable() bool
	isInitializable() bool
}

type ActivityHandler[P any] interface {
	OnActivity(ctx context.Context, c Component[P], state State, err error)
}

type ActivityHandlerFunc[P any] func(ctx context.Context, c Component[P], state State, err error)

func (f ActivityHandlerFunc[P]) OnActivity(ctx context.Context, c Component[P], state State, err error) {
	f(ctx, c, state, err)
}

type Component[P any] interface {
	relation

	Configure(ctx context.Context, options ...app.Option) error
	Initialize(ctx context.Context, options ...app.Option) error
	Activate(ctx context.Context, handler ...ActivityHandler[P]) error
	Deactivate(ctx context.Context, handler ...ActivityHandler[P]) error

	Provider() *P

	EventBus(key interface{}) *event.ManagedEventBus
}

type typedComponent[P any] struct {
	*component
}

func (t typedComponent[P]) AddDependency(dependency relation) relation {
	t.component.AddDependency(dependency)
	return t
}

func (t typedComponent[P]) unwrap() *component {
	return t.component
}

func (c typedComponent[P]) Provider() *P {
	return c.provider.(*P)
}

func (c typedComponent[P]) Activate(ctx context.Context, handler ...ActivityHandler[P]) error {
	return c.activate(ctx, func(ctx context.Context, _ *component, err error) {
		if len(handler) > 0 {
			handler[0].OnActivity(ctx, c, c.state, err)
		}
	})
}

func (c typedComponent[P]) Deactivate(ctx context.Context, handler ...ActivityHandler[P]) error {
	return c.deactivate(ctx, func(ctx context.Context, _ *component, err error) {
		if len(handler) > 0 {
			handler[0].OnActivity(ctx, c, c.state, err)
		}
	})
}

var deactivatoinWaiters = struct {
	sync.Mutex
	*util.ChannelGroup
	cmap   map[interface{}]*component
	cancel func()
}{
	cmap: map[interface{}]*component{},
}

type relations []relation

func cmp(p0 relation, p1 relation) bool {
	return reflect.ValueOf(p0).Pointer() < reflect.ValueOf(p1).Pointer()
}

func (c relations) Add(component relation) relations {
	l := len(c)

	if l == 0 {
		return relations{component}
	}

	var i = l
	for index := 0; index < l; index++ {
		if cmp(c[index], component) && (index == l-1 || !cmp(c[index+1], component)) {
			i = index
			break
		}
	}

	if i == l { // not found = new value is the smallest
		if reflect.ValueOf(c[0]).Pointer() == reflect.ValueOf(component).Pointer() {
			return c
		}
		return append(relations{component}, c...)
	}

	if i == l-1 { // new value is the biggest
		return append(c[0:l], component)
	}

	if reflect.ValueOf(c[i+1]).Pointer() == reflect.ValueOf(component).Pointer() {
		return c
	}

	c = append(c, nil)
	copy(c[i+2:], c[i+1:])
	c[i+1] = component

	return c
}

func (c relations) Remove(component relation) relations {
	l := len(c)

	if l == 0 {
		return c
	}

	var i int
	for i = 0; i < l; i++ {
		if cmp(c[i], component) && (i == l-1 || !cmp(c[i+1], component)) {
			break
		}
	}

	if i == l {
		if reflect.ValueOf(c[0]).Pointer() != reflect.ValueOf(component).Pointer() {
			return c
		}
		return c[1:]
	}

	if i == l-1 { // new value is the biggest
		return c
	}

	if reflect.ValueOf(c[i+1]).Pointer() != reflect.ValueOf(component).Pointer() {
		return c
	}

	if i < l-2 {
		copy(c[i+1:], c[i+2:])
	}
	c[l-1] = nil
	return c[:l-1]
}

func (c relations) Contains(component relation) bool {
	l := len(c)

	if l == 0 {
		return false
	}

	var i = l
	for index := 0; index < l; index++ {
		if cmp(c[index], component) && (index == l-1 || !cmp(c[index+1], component)) {
			i = index + 1
			break
		}
	}

	if i == l {
		return reflect.ValueOf(c[0]).Pointer() == reflect.ValueOf(component).Pointer()
	}
	return reflect.ValueOf(c[i]).Pointer() == reflect.ValueOf(component).Pointer()
}

func (c relations) String() string {
	result := ""

	for _, component := range c {
		if len(result) == 0 {
			result = "["
		} else {
			result = result + ","
		}

		result = result + component.Name()
	}

	if len(result) == 0 {
		result = "[]"
	} else {
		result = result + "]"
	}

	return result
}

func Provider[T any](c *component) T {
	return c.provider.(T)
}

type component struct {
	sync.RWMutex
	componentDomain *componentDomain
	options         []app.Option
	context         context.Context
	id              app.Id
	name            string
	state           State
	dependents      relations
	dependencies    relations
	provider        interface{}
	log             app.Logger
	flags           []cli.Flag
}

func WithFlag[F cli.Flag, T any](flag F, setter func(f F, t *T) bool) app.Option {
	return app.WithOption[any](
		func(a *any) bool {
			switch t := (*a).(type) {
			case *component:
				t.flags = append(t.flags, flag)
				return false
			case *T:
				return setter(flag, t)
			default:
				return false
			}
		})
}

func WithName(name string) app.Option {
	return app.WithOption[component](
		func(c *component) bool {
			c.name = name
			return true
		})
}

type ProviderFactory[P any] interface {
	New() *P
}

type ProviderFactoryFunc[P any] func() *P

func (f ProviderFactoryFunc[P]) New() *P {
	return f()
}

func WithProvider[P any](p *P) app.Option {
	return app.WithOption[component](
		func(c *component) bool {
			c.provider = p
			return true
		})
}

type componentOptions struct {
	dependencies relations
	dependents   relations
	id           string
	logLabels    []string
	logLvl       liblog.Lvl
	logCtx       []interface{}
}

func WithLogLevel(lvl liblog.Lvl) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.logLvl = lvl
			return true
		})
}

func WithLogLabels(labels ...string) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.logLabels = labels
			return true
		})
}

func WithLogCtx(ctx ...interface{}) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.logCtx = ctx
			return true
		})
}

func WithId(id string) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.id = id
			return true
		})
}

func WithDependencies(dependencies ...relation) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.dependencies = append(c.dependencies, dependencies...)
			return true
		})
}

func WithDependent(dependent relation) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.dependents = append(c.dependents, dependent)
			return true
		})
}

func WithDomain(dependent ComponentDomain) app.Option {
	return app.WithOption[componentOptions](
		func(c *componentOptions) bool {
			c.dependents = append(c.dependents, dependent.(*componentDomain).component)
			return true
		})
}

func WithProviderFactory[P any](p ProviderFactory[P]) app.Option {
	return app.WithOption[component](
		func(c *component) bool {
			c.provider = p.New()
			return true
		})
}

func NewComponent[P any](context context.Context, options ...app.Option) (Component[P], error) {
	c := &component{
		RWMutex: sync.RWMutex{},
		context: context,
		state:   Instantiated,
		log:     log,
	}

	opts := componentOptions{
		logLvl: -1,
	}
	options = app.ApplyOptions(&opts, options)
	var domainOpts domainOptions
	options = app.ApplyOptions(&domainOpts, options)
	c.options = app.ApplyOptions(c, options)

	if c.provider == nil {
		var p P
		c.provider = &p
	}

	if len(c.name) == 0 {
		t := reflect.TypeOf(c.provider)
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		c.name = t.String()
	}

	if domainOpts.dependent != nil {
		opts.dependents = append(opts.dependents, domainOpts.dependent.component)
	}

	if len(opts.dependents) > 0 {
		for _, dependent := range opts.dependents {
			if err := c.addDependent(asComponent(dependent), false); err != nil {
				return nil, err
			}
		}
	}

	if c.componentDomain == nil {
		if c.provider == rootComponentDomain {
			if len(opts.id) == 0 {
				opts.id = "root"
			}
			c.id, _ = rootComponentDomain.NewId(context, opts.id)
		} else {
			if err := c.setDomain(rootComponentDomain, false); err != nil {
				return nil, err
			}
		}
	}

	if c.id == nil {
		if len(opts.id) == 0 {
			opts.id = strings.ToLower(filepath.Ext(reflect.TypeOf(c.provider).String())[1:])
		}
		c.id, _ = c.componentDomain.NewId(context, opts.id)
	}

	if err := c.registerSubscriptions(); err != nil {
		return nil, err
	}

	// don't do this for component domains - they will not
	// be fully initialized yet - they handle this themselves
	if _, ok := c.provider.(*componentDomain); !ok {
		for _, dependency := range opts.dependencies {
			c.AddDependency(dependency)
		}
	}

	c.log = log.New(opts.logCtx...).(app.Logger)
	if len(opts.logLabels) > 0 {
		c.log.SetLabels(opts.logLabels...)
	} else {
		c.log.SetLabels(c.Id().String())
	}
	if opts.logLvl >= 0 {
		c.log.SetLevel(opts.logLvl)
	}

	return typedComponent[P]{c}, nil
}

func (c *component) isConfigurable() bool {
	_, ok := c.provider.(Configurable)
	return ok
}

func (c *component) isInitializable() bool {
	_, ok := c.provider.(Initializable)
	return ok
}

func (c *component) String() string {
	if provider, ok := c.provider.(interface{ String() string }); ok {
		return provider.String()
	}

	return fmt.Sprintf("%p:%T [id=%s, name=%s, dependents=%s, state=%s]",
		c,
		c.provider,
		c.Id(),
		c.Name(),
		c.dependents,
		c.State())
}

func (c *component) Context() context.Context {
	if c.context != nil {
		return c.context
	}

	return context.Background()
}

func (c *component) State() State {
	c.RLock()
	state := c.state
	c.RUnlock()
	return state
}

func (c *component) Domain() ComponentDomain {
	return c.componentDomain
}

func (c *component) setState(state State, locked bool) {
	if !locked {
		c.Lock()
		defer c.Unlock()
	}

	if state != c.state {
		previousState := c.state
		c.state = state

		if c.log.DebugEnabled() {
			c.log.Debug("", "provider", app.LogInstance(c.provider), "state", state.String())
		}
		c.serviceBus().Post(newComponentStateChanged(c, state, previousState))
	}
}

func (c *component) AwaitState(ctx context.Context, state State) (State, error) {
	statechan := make(chan State, 1)
	c.RLock()

	if state == c.state {
		c.RUnlock()
		return state, nil
	}

	var subscriber func(event *ComponentStateChanged)

	subscriber = func(event *ComponentStateChanged) {
		sourceComponent, isComponent := event.Source().(relation)

		if !isComponent {
			return
		}

		if sourceComponent == c && event.Current() == state {
			//fmt.Printf("%p Await Received Component State Changed [%v->%v]\n",
			//	c, event.Previous(), event.Current())
			_ = c.serviceBus().Unregister(c, subscriber)
			statechan <- state
		}
	}

	err := c.serviceBus().Register(c, subscriber)

	if err != nil {
		return Unknown, err
	}

	c.RUnlock()
	//fmt.Printf("%T:%p (%v) Await: %v\n", component, component, component.State(), state)
	result, ok := <-statechan

	if !ok {
		return Unknown, fmt.Errorf("subscription channel unexpectedly closed")
	}

	return result, nil
}

func asComponent(r relation) *component {
	switch r := r.(type) {
	case *component:
		return r
	case interface{ unwrap() *component }:
		return r.unwrap()
	}

	return nil
}

func (c *component) AddDependency(dependency relation) relation {
	asComponent(dependency).addDependent(c, false)
	return c
}

func (c *component) RemoveDependency(dependency relation) {
	c.removeDependency(asComponent(dependency), false)
}

func (c *component) Id() app.Id {
	return c.id
}

func (c *component) Name() string {
	if c == nil {
		return "nil"
	}

	if len(c.name) > 0 {
		return c.name
	}

	if c.id == nil || len(c.id.String()) == 0 {
		if c.provider != nil {
			return fmt.Sprintf("%T(%p)", c, c)
		}
		return reflect.TypeOf(c).String()
	}

	return c.id.String()
}

func (c *component) InstanceId() string {
	return app.LogInstance(c)
}

func (c *component) Flags() []cli.Flag {
	c.RLock()
	defer c.RUnlock()

	var flags []cli.Flag

	flags = append(flags, c.flags...)

	for _, dependency := range c.dependencies {
		flags = append(flags, dependency.Flags()...)
	}

	return flags
}

// lock to ensure that only one component collects and activates
// at a time - to avoid multiple activations of subsidiary components
var activationMutex = sync.Mutex{}

func (c *component) Configure(ctx context.Context, options ...app.Option) error {
	return c.configure(ctx, true, false, noopHanlder, options...)
}

func (c *component) configure(ctx context.Context, force bool, activationLocked bool, onActivity onActivity, options ...app.Option) error {
	if !activationLocked {
		activationMutex.Lock()
		defer activationMutex.Unlock()
	}

	err := func() error {
		c.RLock()
		defer c.RUnlock()

		var errs []error

		for _, dependency := range c.dependencies {
			if force || !dependency.State().IsConfigured() {
				if force || dependency.State() == Instantiated ||
					dependency.State() == Unknown {
					if dependency.isConfigurable() {
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

							return asComponent(dependency).configure(ctx, force, true, noopHanlder, options...)
						}()

						if err != nil {
							errs = append(errs, err)
							continue
						}
					}
				}
			}
		}

		if len(errs) > 0 {
			return errors.Join(append(
				[]error{fmt.Errorf("depenccy configure failed for: %s", c.id)}, errs...)...)
		}

		return nil
	}()

	if err != nil {
		return err
	}

	if force || !c.State().IsConfigured() {
		if err := c.configureProvider(ctx, onActivity, options...); err != nil {
			return err
		}
	}

	return nil
}

func (c *component) configureProvider(ctx context.Context, onActivity onActivity, options ...app.Option) error {
	c.Lock()
	defer c.Unlock()

	if configurable, ok := c.provider.(Configurable); ok {
		if _, ok := c.provider.(*componentDomain); !ok {
			if err := configurable.Configure(
				withComponent(ctx, c), append(c.options, options...)...); err != nil {
				return err
			}
		}
	}

	if c.state == Instantiated || c.state == Unknown {
		c.setState(Configured, true)
		onActivity(ctx, c, nil)
	}

	return nil
}

func (c *component) Initialize(ctx context.Context, options ...app.Option) error {
	return c.initialize(ctx, false, noopHanlder, options...)
}

func (c *component) initialize(ctx context.Context, activationLocked bool, onActivity onActivity, options ...app.Option) error {
	if !activationLocked {
		activationMutex.Lock()
		defer activationMutex.Unlock()
	}

	err := func() error {
		c.RLock()
		defer c.RUnlock()

		var errs []error

		for _, dependency := range c.dependencies {
			if !dependency.State().IsInitialized() {
				if dependency.State() == Instantiated ||
					dependency.State() == Configured ||
					dependency.State() == Unknown {
					if dependency.isInitializable() {
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

							return asComponent(dependency).initialize(ctx, true, noopHanlder, options...)
						}()

						if err != nil {
							errs = append(errs, err)
							continue
						}
					}
				}
			}
		}

		if len(errs) > 0 {
			return errors.Join(append(
				[]error{fmt.Errorf("dependency initialize failed for: %s", c.id)}, errs...)...)
		}

		return nil
	}()

	if err != nil {
		return err
	}

	if !c.State().IsInitialized() {
		if err := c.initializeProvider(ctx, onActivity, options...); err != nil {
			return err
		}
	}

	return nil
}

func (c *component) initializeProvider(ctx context.Context, onActivity onActivity, options ...app.Option) error {
	c.Lock()
	defer c.Unlock()

	if configurable, ok := c.provider.(Initializable); ok {
		if _, ok := c.provider.(*componentDomain); !ok {
			if err := configurable.Initialize(withComponent(ctx, c), append(c.options, options...)...); err != nil {
				return err
			}
		}
	}

	if c.state == Configured || c.state == Instantiated || c.state == Unknown {
		c.setState(Initialised, true)
		onActivity(ctx, c, nil)
	}

	return nil
}

type onActivity func(ctx context.Context, c *component, err error)

var noopHanlder = func(context.Context, *component, error) {}

func (c *component) activate(ctx context.Context, onActivity onActivity) error {
	activationList := make([]*component, 0, len(c.dependencies))

	err := func() error {
		activationMutex.Lock()
		defer activationMutex.Unlock()

		if err := c.configure(ctx, false, true, onActivity); err != nil {
			return err
		}

		if err := c.initialize(ctx, true, onActivity); err != nil {
			return err
		}

		c.RLock()
		defer c.RUnlock()

		for _, dependency := range c.dependencies {
			if !dependency.State().IsActivated() {
				activationList = append(activationList, asComponent(dependency))
			}
		}

		return nil
	}()

	if err != nil {
		return err
	}

	if c.State().IsActivated() {
		if len(activationList) == 0 {
			return nil
		} else {
			go c.activateDependencies(ctx, activationList, onActivity)
		}
	} else {
		c.setState(Activating, false)
		onActivity(ctx, c, err)
		go c.activateDependencies(ctx, activationList, onActivity)
	}

	return nil
}

func (c *component) activateDependencies(ctx context.Context, activationList []*component, onActivity onActivity) {
	if len(activationList) == 0 {
		if c.log.TraceEnabled() {
			c.log.Trace("Activated", "component", app.LogInstance(c))
		}
		c.onDependenciesActive(ctx, onActivity)
	} else {
		wg, activationCtx := errgroup.WithContext(ctx)

		for _, dependency := range activationList {
			dependency := dependency

			wg.Go(func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						if rerr, ok := r.(error); ok {
							err = pkg_errors.WithStack(fmt.Errorf("%T Panicked with error: %w", dependency, rerr))
						} else {
							pkg_errors.WithStack(fmt.Errorf("%T Panicked: %v", dependency, r))
						}
					}
				}()

				if c.log.TraceEnabled() {
					c.log.Trace("Activating",
						"component", app.LogInstance(c),
						"dependency", app.LogInstance(dependency))
				}

				cerr := make(chan error, 1)
				dependency.activate(activationCtx, func(ctx context.Context, _ *component, err error) {
					if errors.Is(err, context.Canceled) {
						err = nil
					}
					cerr <- err
				})
				return <-cerr
			})
		}

		err := wg.Wait()
		if c.log.TraceEnabled() {
			c.log.Trace("Activated dependencies",
				"domain", app.LogInstance(c))
		}
		if err != nil {
			onActivity(ctx, c, fmt.Errorf("component activate failed: %w", err))
		} else {
			allDependenciesActivated := true
			c.RLock()
			for _, dependent := range c.dependencies {
				if dependent.State() != Active {
					allDependenciesActivated = false
					break
				}
			}
			c.RUnlock()

			if allDependenciesActivated {
				c.onDependenciesActive(ctx, onActivity)
			}
		}
	}
}

func (c *component) activateProvider(ctx context.Context, onActivity onActivity) {
	//fmt.Println("activateProvider", c.Id())
	if c.context.Err() != nil {
		c.setState(Deactivated, false)
		onActivity(ctx, c, c.context.Err())
		return
	}

	// TODO - not sure if this is usefukl - as
	// it can leave things hanging - needs invetigation
	//if ctx.Err() != nil {
	//	onActivity(ctx, c, c.context.Err())
	//	return
	//}

	if activatable, ok := c.provider.(Activatable); ok {
		err := activatable.Activate(withComponent(ctx, c))

		if err != nil {
			c.setState(Failed, false)
			onActivity(ctx, c, err)
			return
		}
	}
	c.setState(Active, false)

	deactivatoinWaiters.Lock()
	deactivatoinWaiters.cmap[c.context.Done()] = c
	deactivatoinWaiters.Unlock()
	awaitDeactivationChannels()

	onActivity(ctx, c, nil)
}

func awaitDeactivationChannels() {
	deactivatoinWaiters.Lock()
	defer deactivatoinWaiters.Unlock()

	if deactivatoinWaiters.ChannelGroup != nil {
		deactivatoinWaiters.cancel()
	}

	if len(deactivatoinWaiters.cmap) == 0 {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	deactivatoinWaiters.ChannelGroup = util.NewChannelGroup(ctx)
	deactivatoinWaiters.cancel = cancel

	for dc := range deactivatoinWaiters.cmap {
		deactivatoinWaiters.Add(dc)
	}

	waiters := &deactivatoinWaiters
	go func() {
		for wait := true; wait; {
			waiters.Lock()
			channels := waiters.ChannelGroup
			waiters.Unlock()

			if channels == nil {
				break
			}

			wait = channels.Wait(
				func(ch interface{}, _ interface{}, _ bool) (bool, bool) {
					waiters.Lock()
					c, ok := waiters.cmap[ch]
					delete(waiters.cmap, ch)
					waiters.Unlock()

					if ok {
						c.deactivate(context.Background(), noopHanlder)
					}
					return false, false
				}, nil)
		}
		waiters.Lock()
		defer waiters.Unlock()
		waiters.ChannelGroup = nil
		waiters.cancel = nil
	}()
}

func (c *component) deactivate(ctx context.Context, onActivity onActivity) error {
	if c.state.IsDeactivated() {
		onActivity(ctx, c, nil)
		return nil
	}

	go func() {
		if err := c.deactivateProvider(ctx, onActivity); err == nil {
			c.deactivateDependencies(ctx, onActivity)
		}
	}()

	return nil
}

func (c *component) deactivateDependencies(ctx context.Context, onActivity onActivity) {
	deactivationList := make([]*component, 0, len(c.dependencies))

	c.RLock()
DEPENDENCIES:
	for _, dependency := range c.dependencies {
		dependency := asComponent(dependency)
		for _, dependent := range dependency.dependents {
			dependent := asComponent(dependent)
			if !dependent.state.IsDeactivated() {
				continue DEPENDENCIES
			}
		}
		deactivationList = append(deactivationList, dependency)
	}
	c.RUnlock()

	if len(deactivationList) == 0 {
		c.onDependenciesDeactivated(ctx)
		onActivity(ctx, c, nil)
	} else {
		wg, deactivationCtx := errgroup.WithContext(ctx)

		for _, dependency := range deactivationList {
			dependency := dependency

			wg.Go(func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						if rerr, ok := r.(error); ok {
							err = pkg_errors.WithStack(fmt.Errorf("%T Panicked with error: %w", dependency, rerr))
						} else {
							err = pkg_errors.WithStack(fmt.Errorf("%T Panicked: %v", dependency, r))
						}
					}
				}()

				if dependency.State().IsDeactivated() {
					return nil
				}

				cerr := make(chan error, 1)
				dependency.deactivate(deactivationCtx, func(ctx context.Context, _ *component, err error) {
					if errors.Is(err, context.Canceled) {
						err = nil
					}
					cerr <- err
				})
				return <-cerr
			})
		}

		err := wg.Wait()

		if err != nil {
			onActivity(ctx, c, fmt.Errorf("deactivate dependencys failed: %w", err))
		} else {
			_, err := c.AwaitState(ctx, Deactivated)
			onActivity(ctx, c, err)
		}
	}
}

func (c *component) deactivateProvider(ctx context.Context, onActivity onActivity) error {
	c.RLock()
	isActive := !(c.State().IsDeactivated())
	c.RUnlock()

	if isActive {
		c.setState(Deactivating, false)
		onActivity(ctx, c, nil)

		//fmt.Printf("%T:%p Deactivating: %v\n", component, component, component.State())

		if deactivatable, ok := c.provider.(Deactivatable); ok {
			err := deactivatable.Deactivate(withComponent(ctx, c))

			if err != nil {
				c.setState(Failed, false)
				onActivity(ctx, c, err)
				return err
			}

			deactivatoinWaiters.Lock()
			delete(deactivatoinWaiters.cmap, c.context.Done())
			deactivatoinWaiters.Unlock()
			awaitDeactivationChannels()
		}
	}
	return nil
}

func (c *component) EventBus(key interface{}) *event.ManagedEventBus {
	return c.serviceBus().GetEventBus(key)
}

func (c *component) serviceBus() *event.ServiceBus {
	if domain, isDomain := (c.provider.(ComponentDomain)); isDomain {
		return domain.serviceBus()
	}
	if c.componentDomain != nil {
		return c.componentDomain.serviceBus()
	}
	return nil
}

func (c *component) setDomain(cm *componentDomain, domainLocked bool) error {
	if c.componentDomain != cm {
		var registrations []interface{}

		if c.componentDomain != nil {
			registrations = c.serviceBus().Registrations(c)
			if err := c.serviceBus().UnregisterAll(c); err != nil {
				return err
			}
			c.componentDomain.removeDependency(c, domainLocked)
		}
		c.componentDomain = cm
		if cm != nil {
			if c.id != nil {
				c.id, _ = cm.NewId(context.Background(), c.id.Value())
				if c.log != nil {
					c.log.SetLabels(c.id.String())
				}
			}

			if len(c.dependents) == 0 {
				cm.addDependency(c, domainLocked)
			}

			if err := cm.serviceBus().Register(c, registrations...); err != nil {
				return err
			}

			if c.HasDependencies() {
				for _, dependent := range c.Dependencies() {
					if err := asComponent(dependent).setDomain(cm, domainLocked); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (c *component) hasDomain() bool {
	return c.componentDomain != nil
}

func (c *component) HasDependent(dependent relation) bool {
	return c.dependents.Contains(asComponent(dependent))
}

func (c *component) Dependents() relations {
	return c.dependents
}

func (c *component) addDependent(dependent *component, parentLocked bool) error {
	c.dependents = c.dependents.Add(dependent)

	dependent.addDependency(c, parentLocked)

	if domain, ok := dependent.provider.(*componentDomain); ok {
		if c.Domain() != domain {
			return c.setDomain(domain, parentLocked)
		}
	} else {
		if c.Domain() != dependent.Domain() {
			return c.setDomain(dependent.Domain().(*componentDomain), false)
		}
	}

	if c.state != dependent.state {
		switch {
		case dependent.state.IsActivated():
			c.activate(c.context, noopHanlder)
		case dependent.state.IsInitialized():
			c.initialize(c.context, false, noopHanlder)
		case dependent.state.IsConfigured():
			c.initialize(c.context, false, noopHanlder)
		}
	}

	return nil
}

func (c *component) removeDependent(dependent *component, dependentLocked bool) error {
	c.dependents = c.dependents.Remove(dependent)
	dependent.removeDependency(c, dependentLocked)
	return nil
}

func (component *component) registerSubscriptions() error {
	if domain, ok := component.provider.(*componentDomain); ok {
		if serviceBus := domain.serviceBus(); serviceBus != nil {
			return serviceBus.Register(component, component.onComponentStateChanged)
		}
	}

	if !component.hasDomain() {
		return nil
	}

	if serviceBus := component.Domain().serviceBus(); serviceBus != nil {
		return serviceBus.Register(component, component.onComponentStateChanged)
	}

	return fmt.Errorf("expected domain (%T) to have non nil service bus", component.Domain())
}

func (c *component) onComponentStateChanged(event *ComponentStateChanged) {

	sourceComponent, isComponent := event.Source().(relation)

	if !isComponent {
		return
	}

	if sourceComponent != c {
		//fmt.Printf("%s Received Component State Changed [%v->%v] from: %s:%p\n",
		//	c.Id(), event.Previous(), event.Current(), sourceComponent.Id(), sourceComponent)

		var allDependenciesActivated bool
		var allDependenciesDeactivated bool
		var state State

		func() {
			c.RLock()
			defer c.RUnlock()
			state = c.state

			if c.dependencies.Contains(sourceComponent) {
				if event.Current() == Active {
					if state.IsActivated() && state != Active {
						allDependenciesActivated = true
						for _, dependent := range c.dependencies {
							if dependent.State() != Active {
								allDependenciesActivated = false
								break
							}
						}
					}
				}

				if event.Current() == Deactivated {
					if state != Deactivated {
						allDependenciesDeactivated = true
					DEPENDENCIES:
						for _, dependency := range c.dependencies {
							dependency := asComponent(dependency)
							for _, dependent := range dependency.dependents {
								if !asComponent(dependent).state.IsDeactivated() {
									continue DEPENDENCIES
								}
							}
							if dependency.state != Deactivated {
								allDependenciesDeactivated = false
								break
							}
						}
					}
				}
			}
		}()

		if allDependenciesActivated {
			c.onDependenciesActive(c.Context(), noopHanlder)
		}

		if allDependenciesDeactivated && state == Deactivating {
			c.onDependenciesDeactivated(c.Context())
		}
	}
}

func (c *component) onDependenciesActive(ctx context.Context, onActivity onActivity) {
	// this cal get called multiple times as dependencies
	// complete - mke ure that it only activates the
	// provider actions once
	c.Lock()
	activated := c.state == Recovering || c.state == Active
	if !activated {
		c.setState(Recovering, true)
	}
	c.Unlock()

	if activated {
		return
	}

	//fmt.Printf("onDependenciesActive: %s\n", c.Id())

	if recoverable, ok := c.provider.(Recoverable); ok {
		err := recoverable.Recover(withComponent(ctx, c))

		if err != nil {
			c.setState(Failed, false)
			if onActivity != nil {
				onActivity(ctx, c, err)
			}
			return
		}
	}

	c.activateProvider(ctx, onActivity)
}

func (c *component) onDependenciesDeactivated(_ context.Context) {
	c.setState(Deactivated, false)
	if cd, ok := c.provider.(*componentDomain); ok {
		cd.deactivate()
	}
}

func (c *component) addDependency(dependency *component, locked bool) (*component, error) {

	if !locked {
		c.Lock()
		defer c.Unlock()
	}

	if dependency != nil && dependency.provider != nil {
		c.dependencies = c.dependencies.Add(dependency)

		if domain, ok := c.provider.(*componentDomain); ok {
			if dependency.Domain() != domain {
				if err := dependency.setDomain(domain, true); err != nil {
					return nil, err
				}
			}
		}
	}

	return c, nil
}

func (c *component) removeDependency(dependent *component, locked bool) {
	if !locked {
		c.Lock()
		defer c.Unlock()
	}

	if dependent != nil {
		c.dependencies = c.dependencies.Remove(dependent)
	}
}

func (c *component) Dependencies() relations {
	return c.dependencies
}

func (c *component) HasDependencies() bool {
	c.RLock()
	hasDependencies := len(c.dependencies) > 0
	c.RUnlock()
	return hasDependencies
}

func (c *component) GetDependency(context context.Context, selector app.TypedSelector) relation {

	c.RLock()
	defer c.RUnlock()

	for _, dependent := range c.Dependencies() {
		//dependentType := reflect.TypeOf(dependent)
		if /*processorType == dependentType ||*/
		/*(processorType.Kind() == reflect.Interface && dependentType.Implements(processorType)) &&*/ selector.Test(context, dependent) {
			return c
		}
	}

	for _, parent := range c.dependents {
		dependent := parent.GetDependency(context, selector)
		if dependent != nil {
			return dependent
		}
	}

	if c.componentDomain != nil {
		return c.componentDomain.GetDependency(context, selector)
	}

	return nil
}

func (c *component) HasDependency(context context.Context, selector app.TypedSelector) bool {
	return c.GetDependency(context, selector) != nil
}
