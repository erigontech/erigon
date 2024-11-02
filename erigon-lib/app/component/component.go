package component

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/erigontech/erigon-lib/app"
	"github.com/erigontech/erigon-lib/app/event"
	"github.com/erigontech/erigon-lib/app/util"
)

type relation interface {
	Id() app.Id
	Name() string

	State() State
	AwaitState(state State) (State, error)

	Context() context.Context

	Lock()
	Unlock()

	GetDependency(context context.Context, selector app.TypedSelector) relation
	AddDependency(component relation) relation
	RemoveDependency(component relation)

	Dependents() relations
	HasDependent(component relation) bool

	Domain() ComponentDomain
}

type Component[P any] interface {
	relation
	//Config() config.Config

	Provider() *P
	Detach() error
}

func MakeResultChannels() (chan *component, chan error) {
	return make(chan *component, 1), make(chan error, 1)
}

func ReturnResultChannels(c *component, err error) (chan *component, chan error) {
	cproc, cerr := make(chan *component, 1), make(chan error, 1)

	if cproc != nil {
		cproc <- c
	}

	if err != nil {
		cerr <- err
	}

	close(cproc)
	close(cerr)
	return cproc, cerr
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

func AwaitComponent[T any](cres chan *component, cerr chan error) (result Component[T], err error) {
	if cres == nil || cerr == nil {
		return nil, fmt.Errorf("Await Component Failed: channels are undefined")
	}

	resolved := false
	for !resolved {
		select {
		case res, ok := <-cres:
			if ok {
				result = typedComponent[T]{res}
				resolved = true
			}
		case error, ok := <-cerr:
			if ok {
				err = error
				resolved = true
			}
		}
	}

	return result, err
}

type channelMultiplexer struct {
	channels *util.ChannelGroup
}

func ChannelMultiplexer(waitContext context.Context) *channelMultiplexer {
	return &channelMultiplexer{
		channels: util.NewChannelGroup(waitContext),
	}

}

func (mux *channelMultiplexer) Add(cres chan *component, cerr chan error) *channelMultiplexer {
	mux.channels.
		Add(cres).
		Add(cerr)

	return mux
}

func (mux *channelMultiplexer) Context() context.Context {
	return mux.channels.Context()
}

func (mux *channelMultiplexer) Cancel() {
	mux.channels.Cancel()
}

func (mux *channelMultiplexer) Done() bool {
	return mux.channels.Done()
}

func (mux *channelMultiplexer) Await() (results []*component, errors []error) {
	mux.channels.Wait(func(ichan interface{}) (bool, bool) {
		switch crecv := ichan.(type) {
		case chan error:
			err := <-crecv
			errors = append(errors, err)
			return false, false
		case chan *component:
			res := <-crecv
			results = append(results, res)
			return true, false
		default:
			if crecv == mux.channels.Context().Done() {
				errors = append(errors, mux.channels.Context().Err())
				return false, true
			}

			return false, false
		}
	}, nil)

	return results, errors
}

type relations []relation

func cmp(p0 relation, p1 relation) bool {
	//fmt.Printf("[(%p<%p)(%v<%v)=%v]", p0, p1, reflect.ValueOf(p0).Pointer(), reflect.ValueOf(p1).Pointer(),
	//	reflect.ValueOf(p0).Pointer() < reflect.ValueOf(p1).Pointer())
	return reflect.ValueOf(p0).Pointer() < reflect.ValueOf(p1).Pointer()
}

func (c relations) Add(component relation) relations {
	l := len(c)

	//fmt.Printf("Add %p (len=%d)\n", component, l)

	if l == 0 {
		return relations{component}
	}

	/*is := sort.Search(l, func(i int) bool {
		return cmp(c[i], component)
	})*/

	var i = l
	for index := 0; index < l; index++ {
		if cmp(c[index], component) && (index == l-1 || !cmp(c[index+1], component)) {
			//fmt.Printf("\nadd res=%d (%d)\n", index, is)
			i = index
			break
		}
	}

	if i == l { // not found = new value is the smallest
		if reflect.ValueOf(c[0]).Pointer() == reflect.ValueOf(component).Pointer() {
			return c
		}
		//fmt.Printf("\nappend\n")
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
			//fmt.Printf("\nremove res=%d\n", i)
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

	/*fmt.Printf("len=%d", l)
	for _, p := range c {
		fmt.Printf("[%p]", p)
	}
	fmt.Printf("\nprocessor=%v, c:%v\n", component, c)*/

	/*	i := sort.Search(l, cmp )
	 */

	var i = l
	for index := 0; index < l; index++ {
		if cmp(c[index], component) && (index == l-1 || !cmp(c[index+1], component)) {
			//fmt.Printf("\nsearch res=%d\n", index)
			i = index + 1
			break
		}
	}
	//fmt.Printf("search res=%v\n", i)

	if i == l {
		//fmt.Printf("res=%v\n", reflect.ValueOf(c[0]).Pointer() == reflect.ValueOf(component).Pointer())
		return reflect.ValueOf(c[0]).Pointer() == reflect.ValueOf(component).Pointer()
	}
	//fmt.Printf("[%p==%p=%v]\n", c[i], component, reflect.ValueOf(c[i]).Pointer() == reflect.ValueOf(component).Pointer())
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
	options         []Option
	context         context.Context
	id              app.Id
	name            string
	state           State
	dependents      relations
	dependencies    relations
	provider        interface{}
}

type Option struct {
	target     reflect.Type
	applicator func(t interface{})
}

func (o Option) Apply(t interface{}) {
	o.applicator(t)
}

func (o Option) Target() reflect.Type {
	return o.target
}

func WithOption[T any](applicator func(t *T)) Option {
	var t T
	return Option{
		target:     reflect.TypeOf(t),
		applicator: func(t interface{}) { applicator(t.(*T)) },
	}
}

func WithId(id app.Id) Option {
	return WithOption[component](
		func(c *component) {
			c.id = id
		})
}

func WithName(name string) Option {
	return WithOption[component](
		func(c *component) {
			c.name = name
		})
}

type ProviderFactory[P any] interface {
	New() *P
}

type ProviderFactoryFunc[P any] func() *P

func (f ProviderFactoryFunc[P]) New() *P {
	return f()
}

func WithProvider[P any](p *P) Option {
	return WithOption[component](
		func(c *component) {
			c.provider = p
		})
}

func WithDependencies(dependencies ...relation) Option {
	return WithOption[component](
		func(c *component) {
			c.dependencies = append(c.dependencies, dependencies...)
		})
}

func WithDependent(dependent relation) Option {
	return WithOption[component](
		func(c *component) {
			c.dependents = append(c.dependents, dependent)
		})
}

func WithProviderFactory[P any](p ProviderFactory[P]) Option {
	return WithOption[component](
		func(c *component) {
			c.provider = p.New()
		})
}

func ApplyOptions[T any](t *T, options []Option) (remaining []Option) {
	for _, opt := range options {
		if opt.Target() == reflect.TypeOf(t).Elem() {
			opt.Apply(t)
			continue
		}

		remaining = append(remaining, opt)
	}

	return remaining
}

func NewComponent[P any](context context.Context, options ...Option) (Component[P], error) {
	c := &component{
		RWMutex: sync.RWMutex{},
		context: context,
		state:   Instantiated,
	}

	c.options = ApplyOptions(c, options)

	// options may have added a list of dependencies or
	// dependents but we want to add them at the end of
	// this function so the relationship is initialized
	// properly
	var dependencies relations
	if len(c.dependencies) > 0 {
		dependencies = c.dependencies
		c.dependencies = nil
	}

	var dependents relations
	if len(c.dependents) > 0 {
		dependents = c.dependents
		c.dependencies = nil
	}

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

	if len(dependents) > 0 {
		for _, dependent := range dependents {
			if err := c.addDependent(asComponent(dependent), false); err != nil {
				return nil, err
			}
		}
	}

	if len(dependents) == 0 {
		if c.provider != rootComponentDomain {
			if err := c.setDomain(rootComponentDomain, false); err != nil {
				return nil, err
			}
		}
	}

	if c.id == nil {
		c.id, _ = c.componentDomain.NewId(context, strings.ToLower(filepath.Ext(reflect.TypeOf(c.provider).String())[1:]))
	}

	if err := c.registerSubscriptions(); err != nil {
		return nil, err
	}

	for _, dependency := range dependencies {
		c.AddDependency(dependency)
	}

	return typedComponent[P]{c}, nil
}

func (component *component) String() string {
	if provider, ok := component.provider.(interface{ String() string }); ok {
		return provider.String()
	}

	return fmt.Sprintf("%T [id=%s, name=%s, parents=%s, state=%s]",
		component.provider,
		component.Id(),
		component.Name(),
		component.dependents,
		component.State())
}

func (component *component) Context() context.Context {
	if component.context != nil {
		return component.context
	}

	return context.Background()
}

func (component *component) setContext(processorContext context.Context) {
	component.context = processorContext
}

func (component *component) State() State {
	component.RLock()
	state := component.state
	component.RUnlock()
	return state
}

func (component *component) Domain() ComponentDomain {
	return component.componentDomain
}

func (component *component) setState(state State) {
	component.Lock()
	if state != component.state {
		previousState := component.state
		component.state = state
		component.Unlock()

		if log.DebugEnabled() {
			log.Debug().
				Str("provider", app.LogInstance(component.provider)).
				Str("state", state.String()).
				Send()
		}
		component.ServiceBus().Post(newComponentStateChanged(component, state, previousState))
	} else {
		component.Unlock()
	}
}

func (component *component) AwaitState(state State) (State, error) {
	statechan := make(chan State, 1)
	component.RLock()

	if state == component.state {
		component.RUnlock()
		return state, nil
	}

	var subscriber func(event *ComponentStateChanged)

	subscriber = func(event *ComponentStateChanged) {
		sourceComponent, isComponent := event.Source().(relation)

		if !isComponent {
			return
		}

		if sourceComponent == component && event.Current() == state {
			//fmt.Printf("%p Await Received Component State Changed [%v->%v]\n",
			//	component.component(), event.Previous(), event.Current())
			_ = component.ServiceBus().Unregister(component, subscriber)
			statechan <- state
		}
	}

	err := component.ServiceBus().Register(component, subscriber)

	if err != nil {
		return Unknown, err
	}

	component.RUnlock()
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
	c.addDependency(asComponent(dependency), false)
	return c
}

func (c *component) RemoveDependency(dependency relation) {
	c.removeDependency(asComponent(dependency), false)
}

func (component *component) Id() app.Id {
	return component.id
}

func (component *component) Name() string {
	if component == nil {
		return "nil"
	}

	if component.id == nil || len(component.id.String()) == 0 {
		if component.provider != nil {
			return fmt.Sprintf("%T(%p)", component, component)
		}
		return reflect.TypeOf(component).String()
	}

	return component.id.String()
}

func (component *component) InstanceId() string {
	return app.LogInstance(component)
}

func (c *component) activate(activationContext context.Context) (chan *component, chan error) {
	cResOut := make(chan *component, 1)
	cErrOut := make(chan error, 1)

	if activatable, ok := c.provider.(interface {
		Activate(ctx context.Context) (chan *component, chan error)
	}); ok {
		_, err := AwaitComponent[any](activatable.Activate(activationContext))

		if err != nil {
			cErrOut <- err
			close(cResOut)
			close(cErrOut)
			return cResOut, cErrOut
		}
	}

	allActive := true

	c.RLock()

	for _, dependent := range c.dependencies {
		if dependent.State() != Active {
			allActive = false
			break
		}
	}

	c.RUnlock()

	if allActive {
		if err := c.onDependenciesActive(activationContext); err != nil {
			cErrOut <- err
			close(cResOut)
			close(cErrOut)
			return cResOut, cErrOut
		}
	}

	go func() {
		if _, err := c.AwaitState(Active); err != nil {
			cErrOut <- err
		} else {
			cResOut <- c
		}

		close(cResOut)
		close(cErrOut)
	}()

	return cResOut, cErrOut
}

func (c *component) deactivate(deactivationContext context.Context) (chan *component, chan error) {

	cResOut, cErrOut := MakeResultChannels()

	c.RLock()
	isActive := !(c.State().IsDeactivated())
	c.RUnlock()

	if isActive {
		//fmt.Printf("%T:%p Deactivating: %v\n", component, component, component.State())

		// component domains manage their own deactivation state
		if _, ok := c.provider.(*componentDomain); !ok {
			c.setState(Deactivating)
		}

		if deactivatable, ok := c.provider.(Deactivatable); ok {
			err := deactivatable.Deactivate(deactivationContext)

			if err != nil {
				cErrOut <- err
				close(cResOut)
				close(cErrOut)
				return cResOut, cErrOut
			}
		}

		allDeactivated := true

		c.RLock()
		for _, dependent := range c.dependencies {
			if dependent.State() == Active {
				allDeactivated = false
				break
			}
		}
		defer c.RUnlock()

		if allDeactivated {
			//fmt.Printf("1. ")
			if err := c.onDependenciesDeactivated(deactivationContext); err != nil {
				cErrOut <- err
				close(cResOut)
				close(cErrOut)
				return cResOut, cErrOut
			}
		}
	}

	go func() {
		if _, err := c.AwaitState(Deactivated); err != nil {
			cErrOut <- err
		} else {
			//fmt.Printf("%T:%p Deactivated: %v\n", component, component, component.State())
			cResOut <- c
		}
		close(cResOut)
		close(cErrOut)
	}()

	return cResOut, cErrOut
}

func (c *component) Post(args ...interface{}) {
	c.ServiceBus().Post(args...)
}

func (c *component) Register(fns ...interface{}) (err error) {
	return c.ServiceBus().Register(c, fns...)
}

func (c *component) EventBus(key interface{}) *event.ManagedEventBus {
	return c.ServiceBus().GetEventBus(key)
}

func (c *component) ServiceBus() *event.ServiceBus {
	if domain, isDomain := (c.provider.(ComponentDomain)); isDomain {
		return domain.ServiceBus()
	}
	if c.componentDomain != nil {
		return c.componentDomain.ServiceBus()
	}
	return nil
}

func (c *component) setDomain(cm *componentDomain, domainLocked bool) error {
	if c.componentDomain != cm {
		var registrations []interface{}

		if c.componentDomain != nil {
			registrations = c.ServiceBus().Registrations(c)
			if err := c.ServiceBus().UnregisterAll(c); err != nil {
				return err
			}
			c.componentDomain.removeDependency(c, domainLocked)
		}
		c.componentDomain = cm
		if cm != nil {
			if c.id != nil {
				c.id, _ = cm.NewId(context.Background(), c.id.Value())
			}
			cm.addDependency(c, domainLocked)

			if err := cm.ServiceBus().Register(c, registrations...); err != nil {
				return err
			}

			if c.HasDependencies() {
				for _, dependent := range c.Dependencies() {
					if err := asComponent(dependent).setDomain(cm, domainLocked); err != nil {
						return err
					}
				}
			}

			// TODO we may not want to do this - we should just not start until the
			// component is configured (probably via a call in set component status)
			// we'd then be better to add INITIALIZED - so that this is externally
			// controllable i.e. when the state is set to initialized - it will then
			// be started (rather than configured) (start does activate)
			//
			// need to check the domain state and bring the component inline
			if c.State() == Instantiated || c.State() == Unknown {
				//configure(false)
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

	return nil
}

func (c *component) removeDependent(dependent *component, dependentLocked bool) error {
	c.dependents = c.dependents.Remove(dependent)
	dependent.removeDependency(c, dependentLocked)
	return nil
}

func (c *component) Detach() error {
	var errors []error
	for _, dependent := range c.Dependents() {
		if err := c.removeDependent(asComponent(dependent), false); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%d parents failed to detach", len(errors))
	}

	return c.setDomain(nil, false)
}

func (component *component) registerSubscriptions() error {
	if !component.hasDomain() {
		return nil
	}

	if serviceBus := component.Domain().ServiceBus(); serviceBus != nil {
		return serviceBus.Register(component, component.onComponentStateChanged)
	}

	return fmt.Errorf("Expected domain (%T) to have non nil service bus", component.Domain())
}

func (component *component) onComponentStateChanged(event *ComponentStateChanged) {
	//fmt.Printf("%s Received Component State Changed [%v->%v] from: %p\n",
	//	component.Name(), event.Previous(), event.Current(), event.Source())

	sourceComponent, isComponent := event.Source().(relation)

	if !isComponent {
		return
	}

	if sourceComponent != component {
		component.RLock()
		defer component.RUnlock()
		if component.dependencies.Contains(sourceComponent) {
			//	fmt.Printf("%s Received Component State Changed [%v->%v] from: %v\n",
			//		component.Name(), event.Previous(), event.Current(), event.Source())

			if event.Current() == Active {
				/*for _, dependent := range component.dependents {
					fmt.Printf("%p dep: %p: %v\n", component, dependent, dependent)
				}*/

				if !component.State().IsActivated() {
					allActive := true
					for _, dependent := range component.dependencies {
						if dependent.State() != Active {
							allActive = false
							break
						}
					}

					if !allActive {
						return
					}

					//fmt.Printf("2. ")

					if err := component.onDependenciesActive(component.Context()); err != nil {
						log.Debug().
							Caller().
							Str("component", app.LogInstance(component.provider)).
							Err(err).
							Msg("Event handler for : onDependencyComponentsActive failed")
					}
				}
			}

			if event.Current() == Deactivated {
				/*for _, dependent := range component.dependents {
					fmt.Printf("%p dep: %p: %v\n", component, dependent, dependent)
				}*/

				if component.State() != Deactivated {
					allDeactivated := true
					for _, dependent := range component.dependencies {
						if dependent.State() != Deactivated {
							allDeactivated = false
							break
						}
					}

					if !allDeactivated {
						return
					}

					//fmt.Printf("2. ")
					if err := component.onDependenciesActive(component.Context()); err != nil {
						log.Debug("Event handler for : onDependencyComponentsActive failed").
							Caller().
							Str("component", app.LogInstance(component.provider)).
							Err(err)
					}
				}
			}
		}
	}
}

func (component *component) onDependenciesActive(activationContext context.Context) error {
	//fmt.Printf("onDependencyComponentsActive: %v\n", component.component())
	if recoveryProcessor, ok := component.provider.(Recoverable); ok {
		//fmt.Printf("Do Recovery: %v\n", recoveryProcessor)
		component.setState(Recovering)
		go func() {
			err := recoveryProcessor.Recover(activationContext)

			if err == nil {
				component.setState(Active)
			}
		}()
	} else {
		component.setState(Active)
	}

	return nil
}

func (component *component) onDependenciesDeactivated(deactivationContext context.Context) error {
	component.setState(Deactivated)
	return nil
}

func (component *component) addDependency(dependent *component, locked bool) *component {

	if !locked {
		component.Lock()
		defer component.Unlock()
	}

	if dependent != nil && dependent.provider != nil {
		component.dependencies = component.dependencies.Add(dependent)
		/*if component.dependents.Contains(dependent.component()) {
			fmt.Printf("%v ADD DEP dep: %p\n", component.Name(), dependent.component())
			for _, dep := range component.dependents {
				fmt.Printf("  %p %v\n", dep, dep)
			}
		} else {
			fmt.Printf("%v ADD DEP FAILED dep: %p %v\n", component.Name(), dependent.component(), dependent)
			for _, dep := range component.dependents {
				fmt.Printf("  %p  %v\n", dep, dep)
			}
			panic("Shouldn't fail")
		}*/
	}

	return component
}

func (component *component) removeDependency(dependent *component, locked bool) {
	if !locked {
		component.Lock()
		defer component.Unlock()
	}

	if dependent != nil {
		component.dependencies = component.dependencies.Remove(dependent)
	}
}

func (component *component) Dependencies() relations {
	return component.dependencies
}

func (component *component) HasDependencies() bool {
	component.RLock()
	hasDependencies := len(component.dependencies) > 0
	component.RUnlock()
	return hasDependencies
}

func (component *component) GetDependency(context context.Context, selector app.TypedSelector) relation {

	component.RLock()
	defer component.RUnlock()

	for _, dependent := range component.Dependencies() {
		//dependentType := reflect.TypeOf(dependent)
		if /*processorType == dependentType ||*/
		/*(processorType.Kind() == reflect.Interface && dependentType.Implements(processorType)) &&*/ selector.Test(context, dependent) {
			return component
		}
	}

	for _, parent := range component.dependents {
		dependent := parent.GetDependency(context, selector)
		if dependent != nil {
			return dependent
		}
	}

	if component.componentDomain != nil {
		return component.componentDomain.GetDependency(context, selector)
	}

	return nil
}

func (component *component) HasDependency(context context.Context, selector app.TypedSelector) bool {
	return component.GetDependency(context, selector) != nil
}

func (component *component) checkDependencyActivations() bool {
	if component.HasDependencies() {
		component.RLock()
		defer component.RUnlock()

		for _, component := range component.Dependencies() {
			if component.State() != Active {
				return false
			}
		}
	}

	return true
}
