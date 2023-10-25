package cltrace

type InvocationHandler interface {
	Invoke(method string, args []any) (retvals []any, intercept bool)
}
