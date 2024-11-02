package event

import (
	"context"

	"github.com/erigontech/erigon-lib/app"
)

type Cloneable[T any] interface {
	Clone() T
}

type event interface {
	HasSource() bool
	Source() interface{}

	SourceId() app.Id
	HasSourceId() bool
	Name() string

	Context() context.Context

	setSource(source interface{})
	setContext(context context.Context)
}

type Event[E event] interface {
	Cloneable[E]
	event

	WithContext(context context.Context) E
	WithSource(source interface{}) E
}

type UndeliveredEvent struct {
}

type BaseEvent[E event] struct {
	event
	source  interface{}
	context context.Context `json:"-"`
}

func NewBaseEvent[E event](eventContext context.Context, derived E, source interface{}) *BaseEvent[E] {
	return &BaseEvent[E]{derived, source, eventContext}
}

func (event *BaseEvent[E]) CloneBase(derived E) *BaseEvent[E] {
	return &BaseEvent[E]{derived, event.source, event.context}
}

func (event *BaseEvent[E]) HasSource() bool {
	return event.source != nil
}

func (event *BaseEvent[E]) Source() interface{} {
	return event.source
}

func (event *BaseEvent[E]) SourceId() app.Id {
	switch typed := event.Source().(type) {
	case app.Identifiable:
		return typed.Id()
	case app.Id:
		return typed
	}
	return nil
}

func (event *BaseEvent[E]) Context() context.Context {
	if event.context != nil {
		return event.context
	}

	if contextual, ok := event.source.(interface{ Context() context.Context }); ok {
		return contextual.Context()
	}

	return context.Background()
}

func (event *BaseEvent[E]) setContext(context context.Context) {
	event.context = context
}

func (event *BaseEvent[E]) HasSourceId() bool {
	return event.SourceId() != nil
}

func (event *BaseEvent[E]) setSource(source interface{}) {
	event.source = source
}

func (event *BaseEvent[E]) WithSource(source interface{}) E {
	withSource := event.event.(Event[E]).Clone()
	withSource.setSource(source)
	return withSource
}

func (event *BaseEvent[E]) WithContext(eventContext context.Context) E {
	withContext := event.event.(E)

	if event.context != eventContext {
		withContext := event.event.(Event[E]).Clone()
		withContext.setContext(eventContext)
	}

	return withContext
}
