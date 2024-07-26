package heimdall

import "math"

func NewObserverOptions(opts ...ObserverOption) ObserverOptions {
	defaultOptions := ObserverOptions{
		eventsLimit: math.MaxInt,
	}

	for _, o := range opts {
		o(&defaultOptions)
	}

	return defaultOptions
}

type ObserverOptions struct {
	eventsLimit int
}

type ObserverOption func(opts *ObserverOptions)

func WithEventsLimit(eventsLimit int) ObserverOption {
	return func(config *ObserverOptions) {
		config.eventsLimit = eventsLimit
	}
}
