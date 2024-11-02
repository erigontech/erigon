package component

import (
	"context"
)

type ctxkey int

const (
	ckComponentDomain ctxkey = iota
	ckComponent
)

func ComponentValue[P any](context context.Context) Component[P] {
	component, _ := context.Value(ckComponent).(Component[P])
	return component
}

func WithComponent[P any](parent context.Context, component Component[P]) context.Context {
	return context.WithValue(parent, ckComponent, component)
}

func ComponentDomainValue(context context.Context) ComponentDomain {
	componentDomain, _ := context.Value(ckComponentDomain).(ComponentDomain)
	return componentDomain
}

func WithComponentDomain(parent context.Context, componentDomain ComponentDomain) context.Context {
	return context.WithValue(parent, ckComponentDomain, componentDomain)
}
