package keystore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"golang.org/x/crypto/sha3"
)

type Account struct {
	Address common.Address `json:"address"` // Ethereum account address derived from the key
	URL     URL            `json:"url"`     // Optional resource locator within a backend
}

// URL represents the canonical identification URL of a wallet or account.
//
// It is a simplified version of url.URL, with the important limitations (which
// are considered features here) that it contains value-copyable components only,
// as well as that it doesn't do any URL encoding/decoding of special characters.
//
// The former is important to allow an account to be copied without leaving live
// references to the original version, whereas the latter is important to ensure
// one single canonical form opposed to many allowed ones by the RFC 3986 spec.
//
// As such, these URLs should not be used outside of the scope of an Ethereum
// wallet or account.
type URL struct {
	Scheme string // Protocol scheme to identify a capable account backend
	Path   string // Path for the backend to identify a unique entity
}

// parseURL converts a user supplied URL into the accounts specific structure.
func parseURL(url string) (URL, error) {
	parts := strings.Split(url, "://")
	if len(parts) != 2 || parts[0] == "" {
		return URL{}, errors.New("protocol scheme missing")
	}
	return URL{
		Scheme: parts[0],
		Path:   parts[1],
	}, nil
}

// String implements the stringer interface.
func (u URL) String() string {
	if u.Scheme != "" {
		return fmt.Sprintf("%s://%s", u.Scheme, u.Path)
	}
	return u.Path
}

// TerminalString implements the log.TerminalStringer interface.
func (u URL) TerminalString() string {
	url := u.String()
	if len(url) > 32 {
		return url[:31] + ".."
	}
	return url
}

// MarshalJSON implements the json.Marshaller interface.
func (u URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// UnmarshalJSON parses url.
func (u *URL) UnmarshalJSON(input []byte) error {
	var textURL string
	err := json.Unmarshal(input, &textURL)
	if err != nil {
		return err
	}
	url, err := parseURL(textURL)
	if err != nil {
		return err
	}
	u.Scheme = url.Scheme
	u.Path = url.Path
	return nil
}

// Cmp compares x and y and returns:
//
//	-1 if x <  y
//	 0 if x == y
//	+1 if x >  y
func (u URL) Cmp(url URL) int {
	if u.Scheme == url.Scheme {
		return strings.Compare(u.Path, url.Path)
	}
	return strings.Compare(u.Scheme, url.Scheme)
}

// Wallet represents a software or hardware wallet that might contain one or more
// accounts (derived from the same seed).
type Wallet interface {
	// URL retrieves the canonical path under which this wallet is reachable. It is
	// used by upper layers to define a sorting order over all wallets from multiple
	// backends.
	URL() URL

	// Status returns a textual status to aid the user in the current state of the
	// wallet. It also returns an error indicating any failure the wallet might have
	// encountered.
	Status() (string, error)

	// Open initializes access to a wallet instance. It is not meant to unlock or
	// decrypt account keys, rather simply to establish a connection to hardware
	// wallets and/or to access derivation seeds.
	//
	// The passphrase parameter may or may not be used by the implementation of a
	// particular wallet instance. The reason there is no passwordless open method
	// is to strive towards a uniform wallet handling, oblivious to the different
	// backend providers.
	//
	// Please note, if you open a wallet, you must close it to release any allocated
	// resources (especially important when working with hardware wallets).
	Open(passphrase string) error

	// Close releases any resources held by an open wallet instance.
	Close() error

	// Accounts retrieves the list of signing accounts the wallet is currently aware
	// of. For hierarchical deterministic wallets, the list will not be exhaustive,
	// rather only contain the accounts explicitly pinned during account derivation.
	Accounts() []Account

	// Contains returns whether an account is part of this particular wallet or not.
	Contains(account Account) bool

	// Derive attempts to explicitly derive a hierarchical deterministic account at
	// the specified derivation path. If requested, the derived account will be added
	// to the wallet's tracked account list.
	Derive(path DerivationPath, pin bool) (Account, error)

	// SelfDerive sets a base account derivation path from which the wallet attempts
	// to discover non zero accounts and automatically add them to list of tracked
	// accounts.
	//
	// Note, self derivation will increment the last component of the specified path
	// opposed to descending into a child path to allow discovering accounts starting
	// from non zero components.
	//
	// Some hardware wallets switched derivation paths through their evolution, so
	// this method supports providing multiple bases to discover old user accounts
	// too. Only the last base will be used to derive the next empty account.
	//
	// You can disable automatic account discovery by calling SelfDerive with a nil
	// chain state reader.
	SelfDerive(bases []DerivationPath, chain ethereum.ChainReader)

	// SignData requests the wallet to sign the hash of the given data
	// It looks up the account specified either solely via its address contained within,
	// or optionally with the aid of any location metadata from the embedded URL field.
	//
	// If the wallet requires additional authentication to sign the request (e.g.
	// a password to decrypt the account, or a PIN code to verify the transaction),
	// an AuthNeededError instance will be returned, containing infos for the user
	// about which fields or actions are needed. The user may retry by providing
	// the needed details via SignDataWithPassphrase, or by other means (e.g. unlock
	// the account in a keystore).
	SignData(account Account, mimeType string, data []byte) ([]byte, error)

	// SignDataWithPassphrase is identical to SignData, but also takes a password
	// NOTE: there's a chance that an erroneous call might mistake the two strings, and
	// supply password in the mimetype field, or vice versa. Thus, an implementation
	// should never echo the mimetype or return the mimetype in the error-response
	SignDataWithPassphrase(account Account, passphrase, mimeType string, data []byte) ([]byte, error)

	// SignText requests the wallet to sign the hash of a given piece of data, prefixed
	// by the Ethereum prefix scheme
	// It looks up the account specified either solely via its address contained within,
	// or optionally with the aid of any location metadata from the embedded URL field.
	//
	// If the wallet requires additional authentication to sign the request (e.g.
	// a password to decrypt the account, or a PIN code to verify the transaction),
	// an AuthNeededError instance will be returned, containing infos for the user
	// about which fields or actions are needed. The user may retry by providing
	// the needed details via SignTextWithPassphrase, or by other means (e.g. unlock
	// the account in a keystore).
	//
	// This method should return the signature in 'canonical' format, with v 0 or 1.
	SignText(account Account, text []byte) ([]byte, error)

	// SignTextWithPassphrase is identical to Signtext, but also takes a password
	SignTextWithPassphrase(account Account, passphrase string, hash []byte) ([]byte, error)

	// SignTx requests the wallet to sign the given transaction.
	//
	// It looks up the account specified either solely via its address contained within,
	// or optionally with the aid of any location metadata from the embedded URL field.
	//
	// If the wallet requires additional authentication to sign the request (e.g.
	// a password to decrypt the account, or a PIN code to verify the transaction),
	// an AuthNeededError instance will be returned, containing infos for the user
	// about which fields or actions are needed. The user may retry by providing
	// the needed details via SignTxWithPassphrase, or by other means (e.g. unlock
	// the account in a keystore).
	SignTx(account Account, tx types.Transaction, chainID *big.Int) (types.Transaction, error)

	// SignTxWithPassphrase is identical to SignTx, but also takes a password
	SignTxWithPassphrase(account Account, passphrase string, tx types.Transaction, chainID *big.Int) (types.Transaction, error)
}

// DefaultRootDerivationPath is the root path to which custom derivation endpoints
// are appended. As such, the first account will be at m/44'/60'/0'/0, the second
// at m/44'/60'/0'/1, etc.
var DefaultRootDerivationPath = DerivationPath{0x80000000 + 44, 0x80000000 + 60, 0x80000000 + 0, 0}

// DefaultBaseDerivationPath is the base path from which custom derivation endpoints
// are incremented. As such, the first account will be at m/44'/60'/0'/0/0, the second
// at m/44'/60'/0'/0/1, etc.
var DefaultBaseDerivationPath = DerivationPath{0x80000000 + 44, 0x80000000 + 60, 0x80000000 + 0, 0, 0}

// LegacyLedgerBaseDerivationPath is the legacy base path from which custom derivation
// endpoints are incremented. As such, the first account will be at m/44'/60'/0'/0, the
// second at m/44'/60'/0'/1, etc.
var LegacyLedgerBaseDerivationPath = DerivationPath{0x80000000 + 44, 0x80000000 + 60, 0x80000000 + 0, 0}

// DerivationPath represents the computer friendly version of a hierarchical
// deterministic wallet account derivation path.
//
// The BIP-32 spec https://github.com/bitcoin/bips/blob/master/bip-0032.mediawiki
// defines derivation paths to be of the form:
//
//	m / purpose' / coin_type' / account' / change / address_index
//
// The BIP-44 spec https://github.com/bitcoin/bips/blob/master/bip-0044.mediawiki
// defines that the `purpose` be 44' (or 0x8000002C) for crypto currencies, and
// SLIP-44 https://github.com/satoshilabs/slips/blob/master/slip-0044.md assigns
// the `coin_type` 60' (or 0x8000003C) to Ethereum.
//
// The root path for Ethereum is m/44'/60'/0'/0 according to the specification
// from https://github.com/ethereum/EIPs/issues/84, albeit it's not set in stone
// yet whether accounts should increment the last component or the children of
// that. We will go with the simpler approach of incrementing the last component.
type DerivationPath []uint32

// ParseDerivationPath converts a user specified derivation path string to the
// internal binary representation.
//
// Full derivation paths need to start with the `m/` prefix, relative derivation
// paths (which will get appended to the default root path) must not have prefixes
// in front of the first element. Whitespace is ignored.
func ParseDerivationPath(path string) (DerivationPath, error) {
	var result DerivationPath

	// Handle absolute or relative paths
	components := strings.Split(path, "/")
	switch {
	case len(components) == 0:
		return nil, errors.New("empty derivation path")

	case strings.TrimSpace(components[0]) == "":
		return nil, errors.New("ambiguous path: use 'm/' prefix for absolute paths, or no leading '/' for relative ones")

	case strings.TrimSpace(components[0]) == "m":
		components = components[1:]

	default:
		result = append(result, DefaultRootDerivationPath...)
	}
	// All remaining components are relative, append one by one
	if len(components) == 0 {
		return nil, errors.New("empty derivation path") // Empty relative paths
	}
	for _, component := range components {
		// Ignore any user added whitespace
		component = strings.TrimSpace(component)
		var value uint32

		// Handle hardened paths
		if strings.HasSuffix(component, "'") {
			value = 0x80000000
			component = strings.TrimSpace(strings.TrimSuffix(component, "'"))
		}
		// Handle the non hardened component
		bigval, ok := new(big.Int).SetString(component, 0)
		if !ok {
			return nil, fmt.Errorf("invalid component: %s", component)
		}
		mx := math.MaxUint32 - value
		if bigval.Sign() < 0 || bigval.Cmp(big.NewInt(int64(mx))) > 0 {
			if value == 0 {
				return nil, fmt.Errorf("component %v out of allowed range [0, %d]", bigval, mx)
			}
			return nil, fmt.Errorf("component %v out of allowed hardened range [0, %d]", bigval, mx)
		}
		value += uint32(bigval.Uint64())

		// Append and repeat
		result = append(result, value)
	}
	return result, nil
}

// String implements the stringer interface, converting a binary derivation path
// to its canonical representation.
func (path DerivationPath) String() string {
	result := "m"
	for _, component := range path {
		var hardened bool
		if component >= 0x80000000 {
			component -= 0x80000000
			hardened = true
		}
		result = fmt.Sprintf("%s/%d", result, component)
		if hardened {
			result += "'"
		}
	}
	return result
}

// MarshalJSON turns a derivation path into its json-serialized string
func (path DerivationPath) MarshalJSON() ([]byte, error) {
	return json.Marshal(path.String())
}

// UnmarshalJSON a json-serialized string back into a derivation path
func (path *DerivationPath) UnmarshalJSON(b []byte) error {
	var dp string
	var err error
	if err = json.Unmarshal(b, &dp); err != nil {
		return err
	}
	*path, err = ParseDerivationPath(dp)
	return err
}

// DefaultIterator creates a BIP-32 path iterator, which progresses by increasing the last component:
// i.e. m/44'/60'/0'/0/0, m/44'/60'/0'/0/1, m/44'/60'/0'/0/2, ... m/44'/60'/0'/0/N.
func DefaultIterator(base DerivationPath) func() DerivationPath {
	path := make(DerivationPath, len(base))
	copy(path[:], base[:])
	// Set it back by one, so the first call gives the first result
	path[len(path)-1]--
	return func() DerivationPath {
		path[len(path)-1]++
		return path
	}
}

// LedgerLiveIterator creates a bip44 path iterator for Ledger Live.
// Ledger Live increments the third component rather than the fifth component
// i.e. m/44'/60'/0'/0/0, m/44'/60'/1'/0/0, m/44'/60'/2'/0/0, ... m/44'/60'/N'/0/0.
func LedgerLiveIterator(base DerivationPath) func() DerivationPath {
	path := make(DerivationPath, len(base))
	copy(path[:], base[:])
	// Set it back by one, so the first call gives the first result
	path[2]--
	return func() DerivationPath {
		// ledgerLivePathIterator iterates on the third component
		path[2]++
		return path
	}
}

func TextHash(data []byte) []byte {
	hash, _ := TextAndHash(data)
	return hash
}

// TextAndHash is a helper function that calculates a hash for the given message that can be
// safely used to calculate a signature from.
//
// The hash is calculated as
//
//	keccak256("\x19Ethereum Signed Message:\n"${message length}${message}).
//
// This gives context to the signed message and prevents signing of transactions.
func TextAndHash(data []byte) ([]byte, string) {
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(data), string(data))
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write([]byte(msg))
	return hasher.Sum(nil), msg
}

// WalletEventType represents the different event types that can be fired by
// the wallet subscription subsystem.
type WalletEventType int

const (
	// WalletArrived is fired when a new wallet is detected either via USB or via
	// a filesystem event in the keystore.
	WalletArrived WalletEventType = iota

	// WalletOpened is fired when a wallet is successfully opened with the purpose
	// of starting any background processes such as automatic key derivation.
	WalletOpened

	// WalletDropped
	WalletDropped
)

// WalletEvent is an event fired by an account backend when a wallet arrival or
// departure is detected.
type WalletEvent struct {
	Wallet Wallet          // Wallet instance arrived or departed
	Kind   WalletEventType // Event type that happened in the system
}
