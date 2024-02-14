package beacon_router_configuration

import "time"

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
}
