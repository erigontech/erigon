package commitment

// The cross-client conformance vectors from ethereum/execution-specs
// (projects/binary-trie), vendored verbatim as testdata/binary_trie_vectors.json
// and regenerated there by the reference implementation, which hashes with
// BLAKE3.
//
// The four primitive sections pin the embedding piece by piece; pbt_state pins
// their composition — whole accounts to a root, which is where an embedding
// mistake actually surfaces.

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

type pbinConformance struct {
	Source       string `json:"source"`
	SourceCommit string `json:"source_commit"`

	TrieRoots []pbinSpecTrieVector `json:"trie_roots"`

	Embedding struct {
		Address20            string            `json:"address20"`
		Address32            string            `json:"address32"`
		BasicDataKey         string            `json:"basic_data_key"`
		CodeHashKey          string            `json:"code_hash_key"`
		HeaderSubIndex255Key string            `json:"header_sub_index_255_key"`
		StorageSlotKeys      map[string]string `json:"storage_slot_keys"`
		CodeChunkKeys        map[string]string `json:"code_chunk_keys"`
		CodeHash             string            `json:"code_hash"`
	} `json:"embedding"`

	ChunkifyCode []struct {
		Name   string   `json:"name"`
		Code   string   `json:"code"`
		Chunks []string `json:"chunks"`
	} `json:"chunkify_code"`

	EncodeBasicData []struct {
		CodeSize uint64 `json:"code_size"`
		Nonce    uint64 `json:"nonce"`
		Balance  string `json:"balance"`
		Encoded  string `json:"encoded"`
	} `json:"encode_basic_data"`

	PBTState []struct {
		Name     string `json:"name"`
		Accounts map[string]struct {
			Nonce    uint64            `json:"nonce"`
			Balance  string            `json:"balance"`
			Code     string            `json:"code"`
			CodeHash string            `json:"code_hash"`
			Storage  map[string]string `json:"storage"`
		} `json:"accounts"`
		Root string `json:"root"`
	} `json:"pbt_state"`
}

func pbinLoadConformance(t *testing.T) *pbinConformance {
	t.Helper()
	raw, err := os.ReadFile("testdata/binary_trie_vectors.json")
	require.NoError(t, err)
	v := new(pbinConformance)
	require.NoError(t, json.Unmarshal(raw, v))
	require.NotEmpty(t, v.SourceCommit)
	return v
}

func pbinUnhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	require.NoError(t, err)
	return b
}

// pbinSlotBytes parses a slot given as a full decimal expansion, which reaches
// 2**256-1 and so cannot go through a JSON number.
func pbinSlotBytes(t *testing.T, decimal string) []byte {
	t.Helper()
	n, ok := new(big.Int).SetString(decimal, 10)
	require.True(t, ok, "slot %q", decimal)
	var slot [32]byte
	n.FillBytes(slot[:])
	return slot[:]
}

// TestPBinConformanceTrieRoots pins raw trie semantics against the oracle. The
// engine cannot take these: their keys carry synthetic zone bytes chosen to
// exercise bit-level divergence, and the engine only admits allocated zones.
// TestPBinConformancePBTState is where the engine meets the same reference.
func TestPBinConformanceTrieRoots(t *testing.T) {
	for _, c := range pbinLoadConformance(t).TrieRoots {
		t.Run(c.Name, func(t *testing.T) {
			tree := &pbinOracleTree{}
			for _, e := range c.Entries {
				tree.insert(pbinUnhex(t, e.Key), pbinUnhex(t, e.Value))
			}
			got := pbinOracleMerkelizeWith(tree.root, pbinBlake3Sum)
			require.Equal(t, c.Root, "0x"+hex.EncodeToString(got[:]))
		})
	}
}

func TestPBinConformanceEmbedding(t *testing.T) {
	e := pbinLoadConformance(t).Embedding
	addr := pbinUnhex(t, e.Address20)
	codeHash := common.BytesToHash(pbinUnhex(t, e.CodeHash))
	keys := pbinDigestCache{sum: pbinBlake3Hash}

	require.Equal(t, e.Address32, "0x"+hex.EncodeToString(func() []byte {
		a := pbinAddr32(addr)
		return a[:]
	}()))

	hexKey := func(k []byte) string { return "0x" + hex.EncodeToString(k) }
	require.Equal(t, e.BasicDataKey, hexKey(keys.accountKey(addr, pbinBasicDataLeafKey)))
	require.Equal(t, e.CodeHashKey, hexKey(keys.accountKey(addr, pbinCodeHashLeafKey)))
	require.Equal(t, e.HeaderSubIndex255Key, hexKey(keys.accountKey(addr, 255)))

	for slot, want := range e.StorageSlotKeys {
		require.Equal(t, want, hexKey(keys.storageKey(addr, pbinSlotBytes(t, slot))), "slot %s", slot)
	}

	for chunk, want := range e.CodeChunkKeys {
		id, err := strconv.Atoi(chunk)
		require.NoError(t, err)
		var got []byte
		if id < pbinCodeOffset {
			got = keys.codeChunkKey(addr, id)
		} else {
			got = keys.codeOverflowKey(codeHash, id)
		}
		require.Equal(t, want, hexKey(got), "chunk %s", chunk)
	}
}

func TestPBinConformanceChunkifyCode(t *testing.T) {
	for _, c := range pbinLoadConformance(t).ChunkifyCode {
		t.Run(c.Name, func(t *testing.T) {
			chunks := pbinChunkifyCode(pbinUnhex(t, c.Code))
			require.Len(t, chunks, len(c.Chunks))
			for i, want := range c.Chunks {
				require.Equal(t, want, "0x"+hex.EncodeToString(chunks[i][:]), "chunk %d", i)
			}
		})
	}
}

func TestPBinConformanceEncodeBasicData(t *testing.T) {
	for _, c := range pbinLoadConformance(t).EncodeBasicData {
		balance, err := uint256.FromHex(c.Balance)
		require.NoError(t, err)
		got, err := pbinEncodeBasicData(c.Nonce, balance, c.CodeSize)
		require.NoError(t, err)
		require.Equal(t, c.Encoded, "0x"+hex.EncodeToString(got[:]),
			"code_size=%d nonce=%d balance=%s", c.CodeSize, c.Nonce, c.Balance)
	}
}

// TestPBinConformancePBTState rebuilds each reference state leaf by leaf and
// checks the root, through the oracle and through the engine. Two rules decide
// what is not written: a leaf whose value is 32 zero bytes is absent, and code
// length comes from code_size rather than from which chunks exist.
func TestPBinConformancePBTState(t *testing.T) {
	pbinRestoreHashSuite(t)
	require.NoError(t, SetPBinHashSuite(PBinHashBlake3))

	var zero [pbinValueLength]byte
	for _, c := range pbinLoadConformance(t).PBTState {
		t.Run(c.Name, func(t *testing.T) {
			keys := pbinDigestCache{sum: pbinBlake3Hash}
			leaves := map[string][]byte{}
			put := func(key []byte, value [pbinValueLength]byte) {
				if value == zero {
					return
				}
				leaves[string(key)] = value[:]
			}

			for addrHex, acc := range c.Accounts {
				addr := pbinUnhex(t, addrHex)
				code := pbinUnhex(t, acc.Code)
				codeHash := common.BytesToHash(pbinUnhex(t, acc.CodeHash))
				balance, err := uint256.FromHex(acc.Balance)
				require.NoError(t, err)

				basic, err := pbinEncodeBasicData(acc.Nonce, balance, uint64(len(code)))
				require.NoError(t, err)
				put(keys.accountKey(addr, pbinBasicDataLeafKey), basic)
				put(keys.accountKey(addr, pbinCodeHashLeafKey), pbinCodeHashValue(codeHash))

				for i, chunk := range pbinChunkifyCode(code) {
					if i < pbinCodeOffset {
						put(keys.codeChunkKey(addr, i), chunk)
						continue
					}
					put(keys.codeOverflowKey(codeHash, i), chunk)
				}

				for slot, value := range acc.Storage {
					put(keys.storageKey(addr, pbinSlotBytes(t, slot)),
						pbinEncodeStorageValue(pbinUnhex(t, value)))
				}
			}

			ordered := make([]string, 0, len(leaves))
			for k := range leaves {
				ordered = append(ordered, k)
			}
			sort.Strings(ordered)

			tree := &pbinOracleTree{}
			for _, k := range ordered {
				tree.insert([]byte(k), leaves[k])
			}
			got := pbinOracleMerkelizeWith(tree.root, pbinBlake3Sum)
			require.Equal(t, c.Root, "0x"+hex.EncodeToString(got[:]), "oracle, %d leaves", len(leaves))

			if len(leaves) == 0 {
				return // the engine needs a context to load a root it never stored
			}
			// The engine derives chunk and header leaves itself from an account
			// update, so it is driven by accounts and slots rather than by the leaf
			// set above — that is the point of running both.
			corpus := &pbinTestCorpus{codes: map[string][]byte{}}
			for addrHex, acc := range c.Accounts {
				addr := pbinUnhex(t, addrHex)
				code := pbinUnhex(t, acc.Code)
				balance, err := uint256.FromHex(acc.Balance)
				require.NoError(t, err)
				u := Update{
					Flags:    NonceUpdate | BalanceUpdate | CodeUpdate,
					Nonce:    acc.Nonce,
					CodeHash: common.BytesToHash(pbinUnhex(t, acc.CodeHash)),
					CodeSize: uint64(len(code)),
				}
				u.Balance.Set(balance)
				corpus.plainKeys = append(corpus.plainKeys, addr)
				corpus.updates = append(corpus.updates, u)
				corpus.codes[string(addr)] = code

				for slot, value := range acc.Storage {
					trimmed := pbinTrimLeft(pbinUnhex(t, value))
					su := Update{Flags: StorageUpdate, StorageLen: int8(len(trimmed))}
					copy(su.Storage[:], trimmed)
					corpus.plainKeys = append(corpus.plainKeys, append(bytes.Clone(addr), pbinSlotBytes(t, slot)...))
					corpus.updates = append(corpus.updates, su)
				}
			}

			pph, ms := pbinTestEngine(t)
			hasher := pph.setHashSuite(pbinBlake3Hash)
			corpus.applyTo(t, ms)
			upd := WrapKeyUpdates(t, ModeDirect, hasher, corpus.plainKeys, corpus.updates)
			root, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
			require.NoError(t, err)
			require.Equal(t, c.Root, "0x"+hex.EncodeToString(root), "engine")
		})
	}
}

// pbinTrimLeft drops leading zero bytes, the trimmed form the domain layer keeps
// a storage value in.
func pbinTrimLeft(value []byte) []byte {
	i := 0
	for i < len(value) && value[i] == 0 {
		i++
	}
	return value[i:]
}
