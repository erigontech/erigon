package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/execution/chain/networkname"
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

// GlobalConfig is the minimal config stored in the user's config directory.
// It remembers the last-used datadir so `etui` works with no flags.
type GlobalConfig struct {
	DataDir string `toml:"datadir"`
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

// GlobalConfigDir returns the platform-appropriate user config directory
// for etui (e.g. ~/.config/etui on Linux).
func GlobalConfigDir() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		// Fallback to home directory.
		home, _ := os.UserHomeDir()
		return filepath.Join(home, ".config", "etui")
	}
	return filepath.Join(dir, "etui")
}

// GlobalConfigPath returns the full path to the global config file.
func GlobalConfigPath() string {
	return filepath.Join(GlobalConfigDir(), "etui.toml")
}

// LoadGlobal reads the global config to discover the last-used datadir.
// Returns empty string if the file doesn't exist or can't be read.
func LoadGlobal() string {
	data, err := os.ReadFile(GlobalConfigPath())
	if err != nil {
		return ""
	}
	var gc GlobalConfig
	if err := toml.Unmarshal(data, &gc); err != nil {
		return ""
	}
	return gc.DataDir
}

// SaveGlobal writes the datadir to the global config file so future
// bare `etui` invocations find it.
func SaveGlobal(datadir string) error {
	dir := GlobalConfigDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating config dir %s: %w", dir, err)
	}
	gc := GlobalConfig{DataDir: datadir}
	data, err := toml.Marshal(&gc)
	if err != nil {
		return fmt.Errorf("marshalling global config: %w", err)
	}
	path := GlobalConfigPath()
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
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

// SaveAll writes both the per-node config and the global pointer config.
func (c *TUIConfig) SaveAll() error {
	if err := c.Save(); err != nil {
		return err
	}
	return SaveGlobal(c.DataDir)
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
	// Normalise to lowercase — networkname.Supported() is case-insensitive,
	// but the Erigon binary expects lowercase chain names.
	c.Chain = strings.ToLower(c.Chain)
	if !networkname.Supported(c.Chain) {
		return fmt.Errorf("unsupported chain %q (see --help for valid values)", c.Chain)
	}
	return nil
}

// ValidChains returns the list of all chain names supported by Erigon,
// sourced from the canonical networkname.All registry.
func ValidChains() []string {
	// Return a copy so callers can't modify the original.
	chains := make([]string, len(networkname.All))
	copy(chains, networkname.All)
	return chains
}

// ChainDescription returns a human-readable description for a chain name.
func ChainDescription(chain string) string {
	switch chain {
	case networkname.Mainnet:
		return "Ethereum mainnet"
	case networkname.Sepolia:
		return "Sepolia testnet"
	case networkname.Hoodi:
		return "Hoodi testnet"
	case networkname.Mumbai:
		return "Polygon Mumbai testnet (deprecated)"
	case networkname.Amoy:
		return "Polygon Amoy testnet"
	case networkname.BorMainnet:
		return "Polygon PoS mainnet"
	case networkname.BorDevnet:
		return "Polygon devnet"
	case networkname.Gnosis:
		return "Gnosis Chain mainnet"
	case networkname.Chiado:
		return "Gnosis Chiado testnet"
	case networkname.Test:
		return "Internal test chain"
	case networkname.Bloatnet:
		return "Bloatnet test chain"
	default:
		return ""
	}
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

// DefaultDatadir returns a platform-appropriate default data directory for Erigon.
func DefaultDatadir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", "erigon-data")
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Erigon")
	case "windows":
		return filepath.Join(home, "AppData", "Local", "Erigon")
	default:
		return filepath.Join(home, ".local", "share", "erigon")
	}
}

// FormatRPCPort returns the RPC port as a string.
func FormatRPCPort(port int) string {
	return strconv.Itoa(port)
}
