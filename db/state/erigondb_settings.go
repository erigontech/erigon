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
	StepSize                       uint64 `toml:"step_size"`
	StepsInFrozenFile              uint64 `toml:"steps_in_frozen_file"`
	ReferencesInCommitmentBranches *bool  `toml:"references_in_commitment_branches"`
}

// RefsInCommitmentBranches resolves the commitment "references in branches" regime,
// treating an absent (nil) field as config3.DefaultReferencesInCommitmentBranches.
func (s *ErigonDBSettings) RefsInCommitmentBranches() bool {
	if s.ReferencesInCommitmentBranches == nil {
		return config3.DefaultReferencesInCommitmentBranches
	}
	return *s.ReferencesInCommitmentBranches
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
	return ResolveErigonDBSettingsWithRefsDefault(dirs, logger, noDownloader, nil)
}

// ResolveErigonDBSettingsWithRefsDefault is ResolveErigonDBSettings with an optional first-start
// override: when refsFirstStart is non-nil and erigondb.toml is being created, it sets the initial
// references_in_commitment_branches; an existing or downloader-delivered toml wins (override logged, ignored).
func ResolveErigonDBSettingsWithRefsDefault(dirs datadir.Dirs, logger log.Logger, noDownloader bool, refsFirstStart *bool) (*ErigonDBSettings, error) {
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
		if refsFirstStart != nil {
			logger.Info("--commitment.plainValues ignored: erigondb.toml already exists",
				"references_in_commitment_branches", settings.RefsInCommitmentBranches())
		}
		// An absent field is resolved through RefsInCommitmentBranches(); the file is synced
		// snapshot metadata and must not be rewritten.
		logger.Info("erigondb settings", "step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile,
			"references_in_commitment_branches", settings.RefsInCommitmentBranches())
		return settings, nil
	}

	refs := config3.DefaultReferencesInCommitmentBranches
	if refsFirstStart != nil {
		refs = *refsFirstStart
	}

	preverifiedExists, err := dir.FileExist(filepath.Join(dirs.Snap, datadir.PreverifiedFileName))
	if err != nil {
		return nil, err
	}

	// Legacy datadir (Erigon <= 3.3): write legacy settings so erigondb.toml exists on disk.
	if preverifiedExists {
		settings := &ErigonDBSettings{
			StepSize:                       config3.LegacyStepSize,
			StepsInFrozenFile:              config3.LegacyStepsInFrozenFile,
			ReferencesInCommitmentBranches: &refs,
		}
		logger.Info("Creating erigondb.toml with LEGACY settings",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile,
			"references_in_commitment_branches", settings.RefsInCommitmentBranches())
		if err := writeErigonDBSettings(settingsPath, settings); err != nil {
			return nil, err
		}
		return settings, nil
	}

	// Fresh datadir, no preverified.toml: use default settings.
	settings := &ErigonDBSettings{
		StepSize:                       config3.DefaultStepSize,
		StepsInFrozenFile:              config3.DefaultStepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
	}
	if noDownloader {
		// No downloader to provide the real file — write defaults to disk now.
		logger.Info("Initializing erigondb.toml with DEFAULT settings (nodownloader)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile,
			"references_in_commitment_branches", settings.RefsInCommitmentBranches())
		if err := writeErigonDBSettings(settingsPath, settings); err != nil {
			return nil, err
		}
	} else {
		// Downloader will provide the real erigondb.toml during header-chain phase.
		if refsFirstStart != nil {
			logger.Info("--commitment.plainValues set but a downloader will deliver erigondb.toml; the downloaded file wins",
				"requested_references_in_commitment_branches", refs)
		}
		logger.Info("erigondb.toml not found, using defaults (downloader will provide real settings)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile)
	}
	return settings, nil
}
