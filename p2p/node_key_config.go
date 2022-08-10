package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"os"
	"path"

	"github.com/ledgerwatch/erigon/crypto"
)

type NodeKeyConfig struct {
}

func (config NodeKeyConfig) DefaultPath(datadir string) string {
	return path.Join(datadir, "nodekey")
}

func (config NodeKeyConfig) generateKey() (*ecdsa.PrivateKey, error) {
	key, err := crypto.GenerateKey()
	if err != nil {
		err = fmt.Errorf("failed to generate node key: %w", err)
	}
	return key, err
}

func (config NodeKeyConfig) parseHex(hex string) (*ecdsa.PrivateKey, error) {
	key, err := crypto.HexToECDSA(hex)
	if err != nil {
		err = fmt.Errorf("failed to parse node key from %s: %w", hex, err)
	}
	return key, err
}

func (config NodeKeyConfig) load(keyfile string) (*ecdsa.PrivateKey, error) {
	key, err := crypto.LoadECDSA(keyfile)
	if err != nil {
		err = fmt.Errorf("failed to load node key from %s: %w", keyfile, err)
	}
	return key, err
}

func (config NodeKeyConfig) save(keyfile string, key *ecdsa.PrivateKey) error {
	err := os.MkdirAll(path.Dir(keyfile), 0755)
	if err == nil {
		err = crypto.SaveECDSA(keyfile, key)
	}
	if err != nil {
		return fmt.Errorf("failed to save node key to %s: %w", keyfile, err)
	}
	return nil
}

func (config NodeKeyConfig) LoadOrGenerateAndSave(keyfile string) (*ecdsa.PrivateKey, error) {
	// If file exists, try to load it.
	if _, err := os.Stat(keyfile); err == nil {
		return config.load(keyfile)
	}

	// No persistent key found, generate and store a new one.
	key, err := config.generateKey()
	if err != nil {
		return nil, err
	}
	if err := config.save(keyfile, key); err != nil {
		return nil, err
	}
	return key, nil
}

func (config NodeKeyConfig) LoadOrParseOrGenerateAndSave(file, hex, datadir string) (*ecdsa.PrivateKey, error) {
	switch {
	case file != "" && hex != "":
		return nil, fmt.Errorf("P2P node key is set as both file and hex string - these options are mutually exclusive")
	case file != "":
		return config.load(file)
	case hex != "":
		return config.parseHex(hex)
	default:
		return config.LoadOrGenerateAndSave(config.DefaultPath(datadir))
	}
}
