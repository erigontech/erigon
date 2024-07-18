package live

import (
	"encoding/json"
	"errors"

	"github.com/erigontech/erigon/eth/tracers"
)

// init registers itself this packages as a lookup for tracers.
func init() {
	tracers.RegisterLookup(false, lookup)
}

// ctorFn is the constructor signature of a native tracer.
type ctorFn = func(*tracers.Context, json.RawMessage) (*tracers.Tracer, error)

// ctors is a map of package-local tracer constructors.
var ctors map[string]ctorFn

// register is used by native tracers to register their presence.
func register(name string, ctor ctorFn) {
	if ctors == nil {
		ctors = make(map[string]ctorFn)
	}
	ctors[name] = ctor
}

// lookup returns a tracer, if one can be matched to the given name.
func lookup(name string, ctx *tracers.Context, cfg json.RawMessage) (*tracers.Tracer, error) {
	if ctors == nil {
		ctors = make(map[string]ctorFn)
	}
	if ctor, ok := ctors[name]; ok {
		return ctor(ctx, cfg)
	}
	return nil, errors.New("no tracer found")
}
