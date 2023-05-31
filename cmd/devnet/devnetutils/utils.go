package devnetutils

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/log/v3"
)

// ClearDevDB cleans up the dev folder used for the operations
func ClearDevDB(dataDir string, logger log.Logger) error {
	logger.Info("Deleting nodes' data folders")

	nodeNumber := 1
	for {
		nodeDataDir := filepath.Join(dataDir, fmt.Sprintf("%d", nodeNumber))
		fileInfo, err := os.Stat(nodeDataDir)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		if fileInfo.IsDir() {
			if err := os.RemoveAll(nodeDataDir); err != nil {
				return err
			}
			logger.Info("SUCCESS => Deleted", "datadir", nodeDataDir)
		} else {
			break
		}
		nodeNumber++
	}
	return nil
}

// UniqueIDFromEnode returns the unique ID from a node's enode, removing the `?discport=0` part
func UniqueIDFromEnode(enode string) (string, error) {
	if len(enode) == 0 {
		return "", fmt.Errorf("invalid enode string")
	}

	// iterate through characters in the string until we reach '?'
	// using index iteration because enode characters have single codepoints
	var i int
	for i < len(enode) && enode[i] != byte('?') {
		i++
	}

	// if '?' is not found in the enode, return an error
	if i == len(enode) {
		return "", fmt.Errorf("invalid enode string")
	}

	return enode[:i], nil
}

// ParseResponse converts any of the models interfaces to a string for readability
func ParseResponse(resp interface{}) (string, error) {
	result, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}

// HexToInt converts a hexadecimal string to uint64
func HexToInt(hexStr string) uint64 {
	cleaned := strings.ReplaceAll(hexStr, "0x", "") // remove the 0x prefix
	result, _ := strconv.ParseUint(cleaned, 16, 64)
	return result
}

// NamespaceAndSubMethodFromMethod splits a parent method into namespace and the actual method
func NamespaceAndSubMethodFromMethod(method string) (string, string, error) {
	parts := strings.SplitN(method, "_", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid string to split")
	}
	return parts[0], parts[1], nil
}

func HashSlicesAreEqual(s1, s2 []libcommon.Hash) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func BuildLog(hash libcommon.Hash, blockNum string, address libcommon.Address, topics []libcommon.Hash, data hexutility.Bytes, txIndex hexutil.Uint, blockHash libcommon.Hash, index hexutil.Uint, removed bool) models.Log {
	return models.Log{
		Address:     address,
		Topics:      topics,
		Data:        data,
		BlockNumber: hexutil.Uint64(HexToInt(blockNum)),
		TxHash:      hash,
		TxIndex:     txIndex,
		BlockHash:   blockHash,
		Index:       index,
		Removed:     removed,
	}
}

func CompareLogEvents(expected, actual models.Log) ([]error, bool) {
	var errs []error

	switch {
	case expected.Address != actual.Address:
		errs = append(errs, fmt.Errorf("expected address: %v, actual address %v", expected.Address, actual.Address))
	case expected.TxHash != actual.TxHash:
		errs = append(errs, fmt.Errorf("expected txhash: %v, actual txhash %v", expected.TxHash, actual.TxHash))
	case expected.BlockHash != actual.BlockHash:
		errs = append(errs, fmt.Errorf("expected blockHash: %v, actual blockHash %v", expected.BlockHash, actual.BlockHash))
	case expected.BlockNumber != actual.BlockNumber:
		errs = append(errs, fmt.Errorf("expected blockNumber: %v, actual blockNumber %v", expected.BlockNumber, actual.BlockNumber))
	case expected.TxIndex != actual.TxIndex:
		errs = append(errs, fmt.Errorf("expected txIndex: %v, actual txIndex %v", expected.TxIndex, actual.TxIndex))
	case !HashSlicesAreEqual(expected.Topics, actual.Topics):
		errs = append(errs, fmt.Errorf("expected topics: %v, actual topics %v", expected.Topics, actual.Topics))
	}

	return errs, len(errs) == 0
}

func GenerateTopic(signature string) []libcommon.Hash {
	hashed := crypto.Keccak256([]byte(signature))
	return []libcommon.Hash{libcommon.BytesToHash(hashed)}
}

// RandomNumberInRange returns a random number between min and max NOT inclusive
func RandomNumberInRange(min, max uint64) (uint64, error) {
	if max <= min {
		return 0, fmt.Errorf("Invalid range: upper bound %d less or equal than lower bound %d", max, min)
	}

	diff := int64(max - min)

	n, err := rand.Int(rand.Reader, big.NewInt(diff))
	if err != nil {
		return 0, err
	}

	return uint64(n.Int64() + int64(min)), nil
}
