package component

import "context"

// ComponentProvider is a type which provides functionality for a composable
// component.  In practice a type that provides functionality for a component
// can provide zero or more of the contained component interfaces.  If any
// interface is implemented it will be called as the component progresses
// through its lifecycle.
//
// There is no need to implement unused interfaces - the component's
// code performs a runtime check and if an interface is missing the
// component transitions immediately to its next state.
//
// For a fully implemented provider its methods will get called in the
// following order:
//  * Configure
//  * Initialize
//  * Recover
//  * Activate
//  * Deactivate
//
// If any of these methods return a non-nil error then the component will
// enter a failed state.
//
// Configure may be called multiple times - if a provider does not support
// reconfiguration this should be ignored after its first call.
//go:generate mockgen -typed=true -source=./provider.go -destination=./provider_mock.go -package=component . ComponentProvider
type ComponentProvider interface {
	Configurable
	Initializable
	Recoverable
	Activatable
	Deactivatable
}

// Configurable is a provider that can be configured, Configure
// can be called multiple time during the lifecycle of a
// component - it is up to the provider to decide the impact
// of multiple configure called
//
// configure will be called for the first time before a
// component is initialized - it is passed any options which
// have been set on the component whose target is the provider
type Configurable interface {
	Configure(ctx context.Context, options ...Option) error
}

// Initializable is a provider which needs to be initialized
// before it can be activated.  Whereas the configure method
// is intended to be called to set up internal state initialize
// should connect to any external components which need to be
// utilized by recovery and activation
//
// The component framework will ensure that all dependencies
// have been activated before initialize is called.
//
// Initialize is passed the same options as Configure. Providers
// which implement both Configure and Initialize should choose how to
// treat these by each call.
//
// If a provider can only be configured at start-up it is enough
// to implement Initialize.
type Initializable interface {
	Initialize(ctx context.Context, options ...Option) error
}

// Recoverable is a provider which needs perform recovery
// before it can be activated.  Recover will be called
// after Configure and Initialize during a components
// activation stage.  It should return once all recovery
// activity has been completed.
type Recoverable interface {
	Recover(ctx context.Context) error
}

// Activatable is a non passive provider which needs to
// be activated in order to fulfill its requirements.
// Activate should return once the provider is activated
// and should not wait for completion.  Typically this
// involves initiating a processing loop.  However for
// purely reactive components this may involve the
// registration of event handlers instead.
type Activatable interface {
	Activate(ctx context.Context) error
}

// Deactivatable is a component that needs to be
// deactivated to free up resources.  Typically
// this is done a process shut down.  However it is
// also possible to activate/deactivate components
// multiple times while a process remains running.
//
// Note that if a provider is running a looping
// process with select deactivate may not be necessary
// selecting the Done channel of the context passed
// into Activate may be enough to stop processing.
//
// The context cancel method will be called before
// deactivate - so that de-activate can be used to
// free up resources in the knowlege that any loop
// processing will be previously stopped
type Deactivatable interface {
	Deactivate(ctx context.Context) error
}
