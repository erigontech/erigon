package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pelletier/go-toml/v2"
)

// TUIConfig holds all user-configurable settings for the Erigon TUI.
// Persisted as TOML in <datadir>/etui.toml.
type TUIConfig struct {
	DataDir        string `toml:"datadir"`
	Chain          string `toml:"chain"`
	PruneMode      string `toml:"prune_mode"`       // "full", "archive", "minimal"
	RPCEnabled     bool   `toml:"rpc_enabled"`      // whether to pass --http flag
	RPCPort        int    `toml:"rpc_port"`         // --http.port value
	PrivateAPIAddr string `toml:"private_api_addr"` // --private.api.addr value
	DiagnosticsURL string `toml:"diagnostics_url"`  // URL for diagnostics/downloader
}

// Defaults returns a TUIConfig with sane default values.
func Defaults() TUIConfig {
	return TUIConfig{
		Chain:          "mainnet",
		PruneMode:      "full",
		RPCEnabled:     false,
		RPCPort:        8545,
		PrivateAPIAddr: "127.0.0.1:9090",
		DiagnosticsURL: DefaultDownloaderURL,
	}
}

// ConfigPath returns the path to etui.toml inside the given datadir.
func ConfigPath(datadir string) string {
	return filepath.Join(datadir, "etui.toml")
}

// Load reads etui.toml from the datadir. If the file doesn't exist,
// it returns Defaults() with DataDir set to the given path.
func Load(datadir string) (TUIConfig, error) {
	cfg := Defaults()
	cfg.DataDir = datadir

	path := ConfigPath(datadir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // first run — use defaults
		}
		return cfg, fmt.Errorf("reading %s: %w", path, err)
	}

	if err := toml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing %s: %w", path, err)
	}
	// Always override DataDir with the actual directory we loaded from.
	cfg.DataDir = datadir
	return cfg, nil
}

// Save writes the config to etui.toml in the configured datadir.
func (c *TUIConfig) Save() error {
	if c.DataDir == "" {
		return fmt.Errorf("datadir is empty")
	}
	if err := os.MkdirAll(c.DataDir, 0755); err != nil {
		return fmt.Errorf("creating datadir: %w", err)
	}

	data, err := toml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshalling config: %w", err)
	}

	path := ConfigPath(c.DataDir)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

// Validate checks that all config values are within acceptable ranges.
func (c *TUIConfig) Validate() error {
	switch c.PruneMode {
	case "full", "archive", "minimal":
		// ok
	default:
		return fmt.Errorf("invalid prune_mode %q (must be full, archive, or minimal)", c.PruneMode)
	}
	if c.RPCPort < 1 || c.RPCPort > 65535 {
		return fmt.Errorf("rpc_port %d out of range (1-65535)", c.RPCPort)
	}
	if c.Chain == "" {
		return fmt.Errorf("chain cannot be empty")
	}
	return nil
}

// ValidChains returns the list of supported network chains.
func ValidChains() []string {
	return []string{"mainnet", "hoodi", "sepolia"}
}

// ValidPruneModes returns the list of supported prune modes.
func ValidPruneModes() []string {
	return []string{"full", "archive", "minimal"}
}

// PruneModeDescription returns a short description of each prune mode.
func PruneModeDescription(mode string) string {
	switch mode {
	case "full":
		return "Keeps recent state, prunes old history (~2TB)"
	case "archive":
		return "Keeps all history for full archive queries (~3TB+)"
	case "minimal":
		return "Aggressive pruning for minimum storage (~800GB)"
	default:
		return ""
	}
}

// FormatRPCPort returns the RPC port as a string.
func FormatRPCPort(port int) string {
	return strconv.Itoa(port)
}
