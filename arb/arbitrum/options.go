package arbitrum

import (
	"github.com/spf13/pflag"
)

func AddOptionsForNodeForwarderConfig(prefix string, f *pflag.FlagSet) {
	AddOptionsForForwarderConfigImpl(prefix, &DefaultNodeForwarderConfig, f)
}

func AddOptionsForSequencerForwarderConfig(prefix string, f *pflag.FlagSet) {
	AddOptionsForForwarderConfigImpl(prefix, &DefaultSequencerForwarderConfig, f)
}

func AddOptionsForForwarderConfigImpl(prefix string, defaultConfig *ForwarderConfig, f *pflag.FlagSet) {
	f.Duration(prefix+".connection-timeout", defaultConfig.ConnectionTimeout, "total time to wait before cancelling connection")
	f.Duration(prefix+".idle-connection-timeout", defaultConfig.IdleConnectionTimeout, "time until idle connections are closed")
	f.Int(prefix+".max-idle-connections", defaultConfig.MaxIdleConnections, "maximum number of idle connections to keep open")
	f.String(prefix+".redis-url", defaultConfig.RedisUrl, "the Redis URL to recomend target via")
	f.Duration(prefix+".update-interval", defaultConfig.UpdateInterval, "forwarding target update interval")
	f.Duration(prefix+".retry-interval", defaultConfig.RetryInterval, "minimal time between update retries")
}
