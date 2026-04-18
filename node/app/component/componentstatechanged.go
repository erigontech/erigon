// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package component

import (
	"github.com/erigontech/erigon/node/app/event"
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
