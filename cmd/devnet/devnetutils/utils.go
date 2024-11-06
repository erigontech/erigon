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

package devnetutils

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
)

var ErrInvalidEnodeString = errors.New("invalid enode string")

// ClearDevDB cleans up the dev folder used for the operations
func ClearDevDB(dataDir string, logger log.Logger) error {
	logger.Info("Deleting nodes' data folders")

	files, err := dir.ReadDir(dataDir)

	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() || file.Name() == "logs" {
			continue
		}

		nodeDataDir := filepath.Join(dataDir, file.Name())

		_, err := os.Stat(nodeDataDir)

		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		if err := os.RemoveAll(nodeDataDir); err != nil {
			return err
		}

		logger.Info("SUCCESS => Deleted", "datadir", nodeDataDir)
	}

	return nil
}

// HexToInt converts a hexadecimal string to uint64
func HexToInt(hexStr string) uint64 {
	cleaned := strings.ReplaceAll(hexStr, "0x", "") // remove the 0x prefix
	result, _ := strconv.ParseUint(cleaned, 16, 64)
	return result
}

// UniqueIDFromEnode returns the unique ID from a node's enode, removing the `?discport=0` part
func UniqueIDFromEnode(enode string) (string, error) {
	if len(enode) == 0 {
		return "", ErrInvalidEnodeString
	}

	// iterate through characters in the string until we reach '?'
	// using index iteration because enode characters have single codepoints
	var i int
	var ati int

	for i < len(enode) && enode[i] != byte('?') {
		if enode[i] == byte('@') {
			ati = i
		}

		i++
	}

	if ati == 0 {
		return "", ErrInvalidEnodeString
	}

	if _, apiPort, err := net.SplitHostPort(enode[ati+1 : i]); err != nil {
		return "", ErrInvalidEnodeString
	} else {
		if _, err := strconv.Atoi(apiPort); err != nil {
			return "", ErrInvalidEnodeString
		}
	}

	// if '?' is not found in the enode, return the original enode if it has a valid address
	if i == len(enode) {
		return enode, nil
	}

	return enode[:i], nil
}

func RandomInt(max int) int {
	if max == 0 {
		return 0
	}

	var n uint16
	binary.Read(rand.Reader, binary.LittleEndian, &n)
	return int(n) % (max + 1)
}

// NamespaceAndSubMethodFromMethod splits a parent method into namespace and the actual method
func NamespaceAndSubMethodFromMethod(method string) (string, string, error) {
	parts := strings.SplitN(method, "_", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid string to split")
	}
	return parts[0], parts[1], nil
}

func GenerateTopic(signature string) []libcommon.Hash {
	hashed := crypto.Keccak256([]byte(signature))
	return []libcommon.Hash{libcommon.BytesToHash(hashed)}
}

// RandomNumberInRange returns a random number between min and max NOT inclusive
func RandomNumberInRange(_min, _max uint64) (uint64, error) {
	if _max <= _min {
		return 0, fmt.Errorf("Invalid range: upper bound %d less or equal than lower bound %d", _max, _min)
	}

	return uint64(RandomInt(int(_max-_min)) + int(_min)), nil
}
