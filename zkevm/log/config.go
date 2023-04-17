package log

// Config for log
type Config struct {
	// Environment defining the log format ("production" or "development").
	Environment LogEnvironment `mapstructure:"Environment"`
	// Level of log, e.g. INFO, WARN, ...
	Level string `mapstructure:"Level"`
	// Outputs
	Outputs []string `mapstructure:"Outputs"`
}
