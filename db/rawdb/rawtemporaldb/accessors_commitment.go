package rawtemporaldb

import (
	"errors"
	"math"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

type conversionPointSettings struct {
	ConversionBlockNum *uint64 `toml:"conversion_block,omitempty"`
	ConversionTxNum    *uint64 `toml:"conversion_txnum,omitempty"`
}

func readConversionPoint(tx kv.TemporalTx) (blockNum uint64, ok bool, err error) {
	path := filepath.Join(tx.Debug().Dirs().Snap, "erigondb.toml")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var settings conversionPointSettings
	if err := toml.Unmarshal(data, &settings); err != nil {
		return 0, false, err
	}
	blockSet := settings.ConversionBlockNum != nil
	txSet := settings.ConversionTxNum != nil
	if !blockSet && !txSet {
		return 0, false, nil
	}
	if blockSet != txSet {
		return 0, false, errors.New("erigondb.toml: conversion point requires conversion_block and conversion_txnum")
	}
	return *settings.ConversionBlockNum, true, nil
}

func CanUnwindToBlockNum(tx kv.TemporalTx) (uint64, error) {
	minUnwindable, err := changeset.ReadLowestUnwindableBlock(tx)
	if err != nil {
		return 0, err
	}
	if minUnwindable == math.MaxUint64 { // no unwindable block found
		domain := kv.CommitmentDomain
		if provider, ok := tx.AggTx().(interface{ CanonicalCommitmentDomain() kv.Domain }); ok {
			domain = provider.CanonicalCommitmentDomain()
		}
		minUnwindable, err = commitmentdb.LatestBlockNumWithCommitment(tx, domain)
		log.Warn("no unwindable block found from changesets, falling back to latest with commitment", "block", minUnwindable, "err", err)
		if err != nil {
			return 0, err
		}
	}
	if minUnwindable > 0 {
		minUnwindable-- // UnwindTo is exclusive, i.e. (unwindPoint,tip] get unwound
	}
	conversionBlock, ok, err := readConversionPoint(tx)
	if err != nil {
		return 0, err
	}
	if ok && conversionBlock > minUnwindable {
		minUnwindable = conversionBlock
	}
	return minUnwindable, nil
}

func CanUnwindBeforeBlockNum(blockNum uint64, tx kv.TemporalTx) (unwindableBlockNum uint64, ok bool, err error) {
	_minUnwindableBlockNum, err := CanUnwindToBlockNum(tx)
	if err != nil {
		return 0, false, err
	}
	if blockNum < _minUnwindableBlockNum {
		return _minUnwindableBlockNum, false, nil
	}
	return blockNum, true, nil
}
