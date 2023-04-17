package etherman

import (
	"errors"
	"strings"
)

var (
	// ErrGasRequiredExceedsAllowance gas required exceeds the allowance
	ErrGasRequiredExceedsAllowance = errors.New("gas required exceeds allowance")
	// ErrContentLengthTooLarge content length is too large
	ErrContentLengthTooLarge = errors.New("content length too large")
	//ErrTimestampMustBeInsideRange Timestamp must be inside range
	ErrTimestampMustBeInsideRange = errors.New("timestamp must be inside range")
	//ErrInsufficientAllowance insufficient allowance
	ErrInsufficientAllowance = errors.New("insufficient allowance")
	// ErrBothGasPriceAndMaxFeeGasAreSpecified both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified
	ErrBothGasPriceAndMaxFeeGasAreSpecified = errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	// ErrMaxFeeGasAreSpecifiedButLondonNotActive maxFeePerGas or maxPriorityFeePerGas specified but london is not active yet
	ErrMaxFeeGasAreSpecifiedButLondonNotActive = errors.New("maxFeePerGas or maxPriorityFeePerGas specified but london is not active yet")
	// ErrNoSigner no signer to authorize the transaction with
	ErrNoSigner = errors.New("no signer to authorize the transaction with")
	// ErrMissingTrieNode means that a node is missing on the trie
	ErrMissingTrieNode = errors.New("missing trie node")

	errorsCache = map[string]error{
		ErrGasRequiredExceedsAllowance.Error():             ErrGasRequiredExceedsAllowance,
		ErrContentLengthTooLarge.Error():                   ErrContentLengthTooLarge,
		ErrTimestampMustBeInsideRange.Error():              ErrTimestampMustBeInsideRange,
		ErrInsufficientAllowance.Error():                   ErrInsufficientAllowance,
		ErrBothGasPriceAndMaxFeeGasAreSpecified.Error():    ErrBothGasPriceAndMaxFeeGasAreSpecified,
		ErrMaxFeeGasAreSpecifiedButLondonNotActive.Error(): ErrMaxFeeGasAreSpecifiedButLondonNotActive,
		ErrNoSigner.Error():                                ErrNoSigner,
		ErrMissingTrieNode.Error():                         ErrMissingTrieNode,
	}
)

func tryParseError(err error) (error, bool) {
	parsedError, exists := errorsCache[err.Error()]
	if !exists {
		for errStr, actualErr := range errorsCache {
			if strings.Contains(err.Error(), errStr) {
				return actualErr, true
			}
		}
	}
	return parsedError, exists
}
