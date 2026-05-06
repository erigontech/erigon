package state

import (
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
)

const ERIGONDB_SETTINGS_FILE = "erigondb.toml"

type ErigonDBSettings struct {
	StepSize          uint64 `toml:"step_size"`
	StepsInFrozenFile uint64 `toml:"steps_in_frozen_file"`
}

func readErigonDBSettings(path string) (*ErigonDBSettings, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s ErigonDBSettings
	if err := toml.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func writeErigonDBSettings(path string, s *ErigonDBSettings) error {
	data, err := toml.Marshal(s)
	if err != nil {
		return err
	}
	return dir.WriteFileWithFsync(path, data, 0644)
}

// ResolveErigonDBSettings determines the active ErigonDB settings:
//  1. erigondb.toml exists: reads and returns it.
//  2. Legacy datadir (no erigondb.toml, preverified.toml present): writes legacy settings
//     to erigondb.toml and returns them.
//  3. Fresh datadir (neither file present): returns default settings without writing,
//     so the downloader can provide the real erigondb.toml during header-chain phase.
func ResolveErigonDBSettings(dirs datadir.Dirs, logger log.Logger, noDownloader bool) (*ErigonDBSettings, error) {
	settingsPath := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)

	settingsExists, err := dir.FileExist(settingsPath)
	if err != nil {
		return nil, err
	}

	// Read from erigondb.toml.
	if settingsExists {
		logger.Info("Reading DB settings from existing erigondb.toml")
		settings, err := readErigonDBSettings(settingsPath)
		if err != nil {
			return nil, err
		}
		logger.Info("erigondb settings", "step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		return settings, nil
	}

	preverifiedExists, err := dir.FileExist(filepath.Join(dirs.Snap, datadir.PreverifiedFileName))
	if err != nil {
		return nil, err
	}

	// Legacy datadir (Erigon <= 3.3): write legacy settings so erigondb.toml exists on disk.
	if preverifiedExists {
		settings := &ErigonDBSettings{
			StepSize:          config3.LegacyStepSize,
			StepsInFrozenFile: config3.LegacyStepsInFrozenFile,
		}
		logger.Info("Creating erigondb.toml with LEGACY settings",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		if err := writeErigonDBSettings(settingsPath, settings); err != nil {
			return nil, err
		}
		return settings, nil
	}

	// Fresh datadir, no preverified.toml: use default settings.
	settings := &ErigonDBSettings{
		StepSize:          config3.DefaultStepSize,
		StepsInFrozenFile: config3.DefaultStepsInFrozenFile,
	}
	if noDownloader {
		// No downloader to provide the real file — write defaults to disk now.
		logger.Info("Initializing erigondb.toml with DEFAULT settings (nodownloader)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		if err := writeErigonDBSettings(settingsPath, settings); err != nil {
			return nil, err
		}
	} else {
		// Downloader will provide the real erigondb.toml during header-chain phase.
		logger.Info("erigondb.toml not found, using defaults (downloader will provide real settings)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
	}
	return settings, nil
}
