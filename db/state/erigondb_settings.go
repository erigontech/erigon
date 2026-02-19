package state

import (
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/pelletier/go-toml"
)

const ERIGONDB_SETTINGS_FILE = "erigondb.toml"

type ErigonDBSettings struct {
	StepSize          uint64 `toml:"step_size"`
	StepsInFrozenFile uint64 `toml:"steps_in_frozen_file"`
}

// If snapshots/erigondb.toml does not exist, create it with default settings.
// If it exists, read it and return the settings.
//
// In case erigondb.toml does not exist, there 2 possible scenarios for default settings due to historical reasons.
//
// 1 - Up to Erigon 3.3 (inclusive), that file didn't exist; if we detect the datadir comes from such versions, we assume
// "legacy" settings which were previously hardcoded in the binary and explicitly write the file.
// 2 - From 3.4 onwards, that file should be writen by Erigon itself when initializing the datadir (in case of nodownloader)
// or downloaded from ottersync along with all snapshots.
//
// How do we differentiate between these 2 scenarios in case the file is not present? We check for preverified.toml, if it is present,
// that means the datadir is from a legacy version which finished the first sync at least. It is safe to assume it is <= 3.3.
//
// If preverified.toml does not exist, then it means this is a >= 3.4 sync which either nodownloader didn't finish first sync, or
// downloader didn't finish downloading all snapshots (including remote erigondb.toml).
//
// In the future (after the first hardfork after 3.4 release in all chains), it should be possible to assume all users have an
// erigondb.toml file writen in their datadirs and this logic could be simplified and legacy default handling removed.
func CreateOrReadErigonDBSettings(dirs datadir.Dirs, logger log.Logger) (*ErigonDBSettings, error) {
	settingsExists, err := dir.FileExist(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	if err != nil {
		return nil, err
	}

	preverifiedExists, err := dir.FileExist(filepath.Join(dirs.Snap, datadir.PreverifiedFileName))
	if err != nil {
		return nil, err
	}

	// Create, populate, write and return the settings.
	if !settingsExists {
		var settings ErigonDBSettings

		if preverifiedExists {
			// If preverified.toml exists, then it is a legacy datadir; we should assume default legacy settings.
			settings.StepSize = config3.LegacyStepSize
			settings.StepsInFrozenFile = config3.LegacyStepsInFrozenFile
			logger.Info("Creating erigondb.toml with legacy settings")
		} else {
			// If preverified.toml does not exist, then it is a new datadir; we should assume default settings.
			settings.StepSize = config3.DefaultStepSize
			settings.StepsInFrozenFile = config3.DefaultStepsInFrozenFile
			logger.Info("Creating erigondb.toml with default settings")
		}
		logger.Info("erigondb settings", "step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		settingsBytes, err := toml.Marshal(&settings)
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE), settingsBytes, 0644)
		if err != nil {
			return nil, err
		}

		return &settings, nil
	}

	// Read from erigondb.toml
	logger.Info("Reading DB settings from existing erigondb.toml")
	settingsBytes, err := os.ReadFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	if err != nil {
		return nil, err
	}
	var settings ErigonDBSettings
	if err := toml.Unmarshal(settingsBytes, &settings); err != nil {
		return nil, err
	}
	logger.Info("erigondb settings", "step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
	return &settings, nil
}

// ResolveErigonDBSettings determines the active ErigonDB settings:
//  1. Legacy datadir (no erigondb.toml, preverified.toml present): writes legacy settings
//     to erigondb.toml and returns them.
//  2. erigondb.toml exists: reads and returns it.
//  3. Fresh datadir (neither file present): returns default settings without writing,
//     so the downloader can provide the real erigondb.toml during header-chain phase.
func ResolveErigonDBSettings(dirs datadir.Dirs, logger log.Logger, noDownloader bool) (*ErigonDBSettings, error) {
	settingsExists, err := dir.FileExist(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	if err != nil {
		return nil, err
	}

	preverifiedExists, err := dir.FileExist(filepath.Join(dirs.Snap, datadir.PreverifiedFileName))
	if err != nil {
		return nil, err
	}

	// Legacy datadir (Erigon <= 3.3): write legacy settings so erigondb.toml exists on disk.
	if !settingsExists && preverifiedExists {
		settings := ErigonDBSettings{
			StepSize:          config3.LegacyStepSize,
			StepsInFrozenFile: config3.LegacyStepsInFrozenFile,
		}
		logger.Info("Creating erigondb.toml with LEGACY settings",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		settingsBytes, err := toml.Marshal(&settings)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE), settingsBytes, 0644); err != nil {
			return nil, err
		}
		return &settings, nil
	}

	// Read from erigondb.toml.
	if settingsExists {
		logger.Info("Reading DB settings from existing erigondb.toml")
		settingsBytes, err := os.ReadFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
		if err != nil {
			return nil, err
		}
		var settings ErigonDBSettings
		if err := toml.Unmarshal(settingsBytes, &settings); err != nil {
			return nil, err
		}
		logger.Info("erigondb settings", "step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		return &settings, nil
	}

	// Fresh datadir, no preverified.toml: use default settings.
	settings := ErigonDBSettings{
		StepSize:          config3.DefaultStepSize,
		StepsInFrozenFile: config3.DefaultStepsInFrozenFile,
	}
	if noDownloader {
		// No downloader to provide the real file — write defaults to disk now.
		logger.Info("Initializing erigondb.toml with DEFAULT settings (nodownloader)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
		settingsBytes, err := toml.Marshal(&settings)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE), settingsBytes, 0644); err != nil {
			return nil, err
		}
	} else {
		// Downloader will provide the real erigondb.toml during header-chain phase.
		logger.Info("erigondb.toml not found, using defaults (downloader will provide real settings)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
	}
	return &settings, nil
}
