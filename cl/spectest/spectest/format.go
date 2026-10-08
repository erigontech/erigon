package spectest

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

func (r *Format) GetHandler(name string) (Handler, error) {
	val, ok := r.handlers[name]
	if !ok {
		return nil, ErrHandlerNotFound(name)
	}
	return val, nil
}
