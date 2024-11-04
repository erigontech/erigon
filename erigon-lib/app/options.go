package app

import "reflect"

type Option struct {
	target     reflect.Type
	applicator func(t interface{})
}

func (o Option) Apply(t interface{}) {
	o.applicator(t)
}

func (o Option) Target() reflect.Type {
	return o.target
}

func WithOption[T any](applicator func(t *T)) Option {
	var t T
	return Option{
		target:     reflect.TypeOf(t),
		applicator: func(t interface{}) { applicator(t.(*T)) },
	}
}

func ApplyOptions[T any](t *T, options []Option) (remaining []Option) {
	for _, opt := range options {
		if opt.Target() == reflect.TypeOf(t).Elem() {
			opt.Apply(t)
			continue
		}

		remaining = append(remaining, opt)
	}

	return remaining
}
