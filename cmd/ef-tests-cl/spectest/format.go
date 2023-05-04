package spectest

import (
	"golang.org/x/exp/slices"
)

type Format struct {
	handlers map[string]Handler
}

func NewFormat() *Format {
	o := &Format{
		handlers: map[string]Handler{},
	}
	return o
}

func (r *Format) With(name string, handler Handler) *Format {
	r.handlers[name] = handler
	return r
}

func (r *Format) WithFn(name string, handler HandlerFunc) *Format {
	r.handlers[name] = handler
	return r
}

func (r *Format) GetHandlers() []string {
	o := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		o = append(o, k)
	}
	slices.Sort(o)
	return o
}

func (r *Format) GetHandler(name string) (Handler, error) {
	val, ok := r.handlers[name]
	if !ok {
		return nil, ErrHandlerNotFound(name)
	}
	return val, nil
}
