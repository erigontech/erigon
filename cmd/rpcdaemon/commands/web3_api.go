package commands

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/params"
)

// Web3API provides interfaces for the web3_ RPC commands
type Web3API interface {
	ClientVersion() (string, error)
	Sha3(input hexutil.Bytes) hexutil.Bytes
}

type Web3APIImpl struct {
}

// NewWeb3APIImpl returns Web3APIImpl instance
func NewWeb3APIImpl() *Web3APIImpl {
	return &Web3APIImpl{}
}

// ClientVersion returns the node name
func (api *Web3APIImpl) ClientVersion() (string, error) {
	// https://infura.io/docs/ethereum/json-rpc/web3-clientVersion
	return common.MakeName("TurboGeth", params.VersionWithCommit(gitCommit, "")), nil
}

// Sha3 applies the ethereum sha3 implementation on the input.
func (api *Web3APIImpl) Sha3(input hexutil.Bytes) hexutil.Bytes {
	// https://infura.io/docs/ethereum/json-rpc/web3-sha3
	return crypto.Keccak256(input)
}

var (
	gitCommit string
)

// SetGitStrings very hacky way to get these strings into this package
func SetGitStrings(commit string) {
	gitCommit = commit
}
