package beacon_router_configuration

import "time"

// TODO(enriavil1): Make this configurable via flags
type RouterConfiguration struct {
	Active   bool
	Protocol string
	Address  string

	ReadTimeTimeout time.Duration
	IdleTimeout     time.Duration
	WriteTimeout    time.Duration
}
