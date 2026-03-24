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
