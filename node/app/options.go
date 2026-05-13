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

package app

import (
	"reflect"
)

type Option struct {
	target     reflect.Type
	applicator func(t interface{}) bool
}

func (o Option) Apply(t interface{}) bool {
	return o.applicator(t)
}

func (o Option) Target() reflect.Type {
	return o.target
}

func WithOption[T any](applicator func(t *T) bool) Option {
	var t T
	return Option{
		target:     reflect.TypeOf(t),
		applicator: func(t interface{}) bool { return applicator(t.(*T)) },
	}
}

func ApplyOptions[T any](t *T, options []Option) (remaining []Option) {
	for _, opt := range options {
		if opt.Target() == reflect.TypeOf(t).Elem() {
			if opt.Apply(t) {
				continue
			}
		}
		if opt.Target() == nil {
			var i any = t
			if opt.Apply(&i) {
				continue
			}
		}
		remaining = append(remaining, opt)
	}

	return remaining
}
