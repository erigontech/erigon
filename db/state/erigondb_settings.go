package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

const ERIGONDB_SETTINGS_FILE = "erigondb.toml"

const (
	TrieVariantHex = "hex"
	TrieVariantBin = "bin"
)

type ErigonDBSettings struct {
	StepSize                       uint64 `toml:"step_size"`
	StepsInFrozenFile              uint64 `toml:"steps_in_frozen_file"`
	ReferencesInCommitmentBranches *bool  `toml:"references_in_commitment_branches"`
	// TrieVariant is the commitment trie the datadir was created with ("hex" or
	// "bin"); absent means hex. Like every erigondb.toml key it wins over the CLI.
	TrieVariant *string `toml:"trie_variant,omitempty"`
	// TrieHash is H for a "bin" datadir ("keccak" or "blake3"); absent means
	// keccak. Meaningless under "hex", which has no choice of hash.
	TrieHash *string `toml:"trie_hash,omitempty"`
}

// RefsInCommitmentBranches resolves the commitment "references in branches" regime,
// treating an absent (nil) field as config3.DefaultReferencesInCommitmentBranches.
func (s *ErigonDBSettings) RefsInCommitmentBranches() bool {
	if s.ReferencesInCommitmentBranches == nil {
		return config3.DefaultReferencesInCommitmentBranches
	}
	return *s.ReferencesInCommitmentBranches
}

// TrieVariantName resolves the persisted commitment trie variant, treating an
// absent field as the hex trie.
func (s *ErigonDBSettings) TrieVariantName() string {
	if s.TrieVariant == nil || *s.TrieVariant == "" {
		return TrieVariantHex
	}
	return *s.TrieVariant
}

// TrieHashName resolves H for a bin datadir, treating an absent field as Keccak.
func (s *ErigonDBSettings) TrieHashName() string {
	if s.TrieHash == nil || *s.TrieHash == "" {
		return commitment.PBinHashKeccak
	}
	return *s.TrieHash
}

// reconcileTrieVariant applies the datadir's trie variant to the process: a bin
// datadir turns the bin flag on process-wide, and a combination the bin engine
// cannot honour is refused rather than degraded to a wrong-root run.
func reconcileTrieVariant(s *ErigonDBSettings, logger log.Logger) error {
	switch s.TrieVariantName() {
	case TrieVariantBin:
		if s.RefsInCommitmentBranches() {
			return errors.New("trie_variant \"bin\" conflicts with references_in_commitment_branches = true")
		}
		if statecfg.ExperimentalStreamingCommitment || statecfg.ExperimentalParallelCommitment {
			return errors.New("the bin commitment trie is sequential-only; drop --experimental.streaming-commitment / --experimental.parallel-commitment")
		}
		if !statecfg.ExperimentalBinCommitment {
			logger.Info("datadir uses the bin commitment trie; enabling it for this process")
			statecfg.ExperimentalBinCommitment = true
		}
		// The stored hash wins over the flag: every root on disk was built with it,
		// so honouring a differing flag would silently produce a second tree.
		stored := s.TrieHashName()
		if statecfg.BinCommitmentHash != "" && statecfg.BinCommitmentHash != stored {
			return fmt.Errorf("--experimental.bin-commitment.hash=%s: datadir was built with %q; the bin trie needs a fresh datadir to change hash",
				statecfg.BinCommitmentHash, stored)
		}
		// Resolution runs per RPC request and per aggregator open, while the
		// selected suite is read unsynchronized by every engine; only write it
		// when it actually has to change.
		if commitment.PBinHashSuiteName() != stored {
			if err := commitment.SetPBinHashSuite(stored); err != nil {
				return fmt.Errorf("erigondb.toml: %w", err)
			}
		}
	case TrieVariantHex:
		if s.TrieHash != nil {
			return errors.New("erigondb.toml: trie_hash is meaningless under trie_variant \"hex\"")
		}
		if statecfg.ExperimentalBinCommitment {
			return errors.New("--experimental.bin-commitment: datadir was created with the hex commitment trie; the bin trie needs a fresh datadir")
		}
		if statecfg.BinCommitmentHash != "" {
			return errors.New("--experimental.bin-commitment.hash needs --experimental.bin-commitment")
		}
	default:
		return fmt.Errorf("erigondb.toml: unknown trie_variant %q", s.TrieVariantName())
	}
	return nil
}

// ReadErigonDBSettings reads a datadir's erigondb.toml as plain data. Unlike
// ResolveErigonDBSettings it does not apply the file to the process, so a tool
// can inspect a datadir it is not running on.
func ReadErigonDBSettings(dirs datadir.Dirs) (*ErigonDBSettings, error) {
	return readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
}

// WriteErigonDBSettings writes a datadir's erigondb.toml.
func WriteErigonDBSettings(dirs datadir.Dirs, s *ErigonDBSettings) error {
	return writeErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE), s)
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
		if err := reconcileTrieVariant(settings, logger); err != nil {
			return nil, err
		}
		if refsFirstStart != nil {
			logger.Info("--commitment.plainValues ignored: erigondb.toml already exists",
				"references_in_commitment_branches", settings.RefsInCommitmentBranches())
		}
		// An absent field is resolved through RefsInCommitmentBranches(); the file is synced
		// snapshot metadata and must not be rewritten.
		logger.Info("erigondb settings", "step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile,
			"references_in_commitment_branches", settings.RefsInCommitmentBranches(),
			"trie_variant", settings.TrieVariantName(), "trie_hash", settings.TrieHashName())
		return settings, nil
	}

	refs := config3.DefaultReferencesInCommitmentBranches
	if refsFirstStart != nil {
		refs = *refsFirstStart
	}

	var trieVariant, trieHash *string
	if statecfg.ExperimentalBinCommitment {
		v := TrieVariantBin
		trieVariant = &v
		h := statecfg.BinCommitmentHash
		if h == "" {
			h = commitment.PBinHashKeccak
		}
		trieHash = &h
	}

	preverifiedExists, err := dir.FileExist(filepath.Join(dirs.Snap, datadir.PreverifiedFileName))
	if err != nil {
		return nil, err
	}

	// Legacy datadir (Erigon <= 3.3): write legacy settings so erigondb.toml exists on disk.
	if preverifiedExists {
		if statecfg.ExperimentalBinCommitment {
			return nil, errors.New("--experimental.bin-commitment: this datadir already has hex commitment state; the bin trie needs a fresh datadir")
		}
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
		TrieVariant:                    trieVariant,
		TrieHash:                       trieHash,
	}
	if err := reconcileTrieVariant(settings, logger); err != nil {
		return nil, err
	}
	// A bin datadir persists its variant right away even with a downloader running:
	// no published snapshot set carries a bin erigondb.toml, and leaving the variant
	// unpersisted lets the empty preverified.toml that the snapshots stage commits for
	// a chain without published hashes read as a legacy datadir at the next resolve.
	if noDownloader || trieVariant != nil {
		// No downloader to provide the real file — write defaults to disk now.
		logger.Info("Initializing erigondb.toml with DEFAULT settings (nodownloader)",
			"step_size", settings.StepSize, "steps_in_frozen_file", settings.StepsInFrozenFile,
			"references_in_commitment_branches", settings.RefsInCommitmentBranches(),
			"trie_variant", settings.TrieVariantName())
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
