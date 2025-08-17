// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package paths

import (
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

const dirname = "Erigon"

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
			return filepath.Join(home, ".local", "share", strings.ToLower(dirname))
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

func DataDirForNetwork(datadir string, network string) string {
	if datadir != DefaultDataDir() {
		return datadir
	}

	switch network {
	case networkname.Dev:
		return "" // unless explicitly requested, use memory databases
	case networkname.Holesky:
		return networkDataDirCheckingLegacy(datadir, "holesky")
	case networkname.Hoodi:
		return networkDataDirCheckingLegacy(datadir, "hoodi")
	case networkname.Amoy:
		return networkDataDirCheckingLegacy(datadir, "amoy")
	case networkname.BorMainnet:
		return networkDataDirCheckingLegacy(datadir, "bor-mainnet")
	case networkname.BorDevnet:
		return networkDataDirCheckingLegacy(datadir, "bor-devnet")
	case networkname.Sepolia:
		return networkDataDirCheckingLegacy(datadir, "sepolia")
	case networkname.Gnosis:
		return networkDataDirCheckingLegacy(datadir, "gnosis")
	case networkname.Chiado:
		return networkDataDirCheckingLegacy(datadir, "chiado")

	default:
		return datadir
	}
}

// networkDataDirCheckingLegacy checks if the datadir for the network already exists and uses that if found.
// if not checks for a LOCK file at the root of the datadir and uses this if found
// or by default assume a fresh node and to use the nested directory for the network
func networkDataDirCheckingLegacy(datadir, network string) string {
	anticipated := filepath.Join(datadir, network)

	if _, err := os.Stat(anticipated); !os.IsNotExist(err) {
		return anticipated
	}

	legacyLockFile := filepath.Join(datadir, "LOCK")
	if _, err := os.Stat(legacyLockFile); !os.IsNotExist(err) {
		log.Info("Using legacy datadir")
		return datadir
	}

	return anticipated
}
