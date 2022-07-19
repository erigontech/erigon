package paths

import (
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
)

const dirname = "Erigon"

// DefaultSnapDir is the default snap directory to use for the snapshots and 
// by default will be a subdirectory of data directory
func DefaultSnapDir() string {
	// This is the equivalent of filepath.join(DefaultDataDir(), "snapshots")
	// Because passing an empty string will defer to datadir.New()
	return ""
}

// DefaultDataDir is the default data directory to use for the databases and other
// persistence requirements.
func DefaultDataDir() string {
	// Try to place the data folder in the user's home dir
	home := homeDir()
	if home != "" {
		switch runtime.GOOS {
		case "darwin":
			return filepath.Join(home, "Library", dirname)
		case "windows":
			// We used to put everything in %HOME%\AppData\Roaming, but this caused
			// problems with non-typical setups. If this fallback location exists and
			// is non-empty, use it, otherwise DTRT and check %LOCALAPPDATA%.
			fallback := filepath.Join(home, "AppData", "Roaming", dirname)
			appdata := windowsAppData()
			if appdata == "" || isNonEmptyDir(fallback) {
				return fallback
			}
			return filepath.Join(appdata, dirname)
		default:
			if xdgDataDir := os.Getenv("XDG_DATA_HOME"); xdgDataDir != "" {
				return filepath.Join(xdgDataDir, strings.ToLower(dirname))
			}
			return filepath.Join(home, ".local/share", strings.ToLower(dirname))
		}
	}
	// As we cannot guess a stable location, return empty and handle later
	return ""
}

func windowsAppData() string {
	v := os.Getenv("LOCALAPPDATA")
	if v == "" {
		// Windows XP and below don't have LocalAppData. Crash here because
		// we don't support Windows XP and undefining the variable will cause
		// other issues.
		panic("environment variable LocalAppData is undefined")
	}
	return v
}

func isNonEmptyDir(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		return false
	}
	names, _ := f.Readdir(1)
	f.Close()
	return len(names) > 0
}

func homeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	if usr, err := user.Current(); err == nil {
		return usr.HomeDir
	}
	return ""
}
