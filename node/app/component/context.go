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
	"context"

	"github.com/erigontech/erigon/node/app"
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
