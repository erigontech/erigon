package component

import (
	"context"

	"github.com/erigontech/erigon-lib/app"
)

type ctxkey int

const (
	ckComponentDomain ctxkey = iota
	ckComponent
)

func ComponentValue[P any](context context.Context) Component[P] {
	switch c := context.Value(ckComponent).(type) {
	case Component[P]:
		return c
	case *component:
		return typedComponent[P]{c}
	}

	return nil
}

func WithComponent[P any](parent context.Context, component Component[P]) context.Context {
	return context.WithValue(parent, ckComponent, component)
}

func withComponent(parent context.Context, c *component) context.Context {
	return app.WithLogger(context.WithValue(parent, ckComponent, c), c.log)
}

func ComponentDomainValue(context context.Context) ComponentDomain {
	componentDomain, _ := context.Value(ckComponentDomain).(ComponentDomain)
	return componentDomain
}

func WithComponentDomain(parent context.Context, componentDomain ComponentDomain) context.Context {
	return context.WithValue(parent, ckComponentDomain, componentDomain)
}
