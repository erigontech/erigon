package beacon_router_configuration

import (
	"fmt"
	"strings"
	"time"
)

type RouterConfiguration struct {
	Active   bool
	Protocol string
	Address  string
	// Cors data
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowCredentials bool

	ReadTimeTimeout time.Duration
	IdleTimeout     time.Duration
	WriteTimeout    time.Duration

	Beacon     bool
	Builder    bool
	Config     bool
	Debug      bool
	Events     bool
	Node       bool
	Validator  bool
	Lighthouse bool
}

func (r *RouterConfiguration) UnwrapEndpointsList(l []string) error {
	r.Active = len(l) > 0
	for _, v := range l {
		// convert v to lowercase
		v = strings.ToLower(v)
		switch v {
		case "beacon":
			r.Beacon = true
		case "builder":
			r.Builder = true
		case "config":
			r.Config = true
		case "debug":
			r.Debug = true
		case "events":
			r.Events = true
		case "node":
			r.Node = true
		case "validator":
			r.Validator = true
		case "lighthouse":
			r.Lighthouse = true
		default:
			r.Active = false
			r.Beacon = false
			r.Builder = false
			r.Config = false
			r.Debug = false
			r.Events = false
			r.Node = false
			r.Validator = false
			r.Lighthouse = false
			return fmt.Errorf("unknown endpoint for beacon.api: %s. known endpoints: beacon, builder, config, debug, events, node, validator, lighthouse", v)
		}
	}
	return nil
}
