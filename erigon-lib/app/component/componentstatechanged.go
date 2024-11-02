package component

import (
	"github.com/erigontech/erigon-lib/app/event"
)

type ComponentStateChanged struct {
	*event.BaseEvent[*ComponentStateChanged]
	current  State
	previous State
}

func newComponentStateChanged(source *component, current State, previous State) *ComponentStateChanged {
	e := &ComponentStateChanged{nil, current, previous}
	e.BaseEvent = event.NewBaseEvent(source.Context(), e, source)
	return e
}

func (event *ComponentStateChanged) Current() State {
	return event.current
}

func (event *ComponentStateChanged) Previous() State {
	return event.previous
}
