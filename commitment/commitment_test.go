package commitment

import (
	"crypto/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func Test_AccountEncodeDecode(t *testing.T) {
	balance := uint256.NewInt(1002020020)
	acc := &Account{
		Nonce:    1913453,
		CodeHash: []byte{10, 20, 30, 10},
		Balance:  *balance,
	}
	rand.Read(acc.CodeHash[:])

	aux := make([]byte, 0)
	aux = acc.encode(aux)
	require.NotEmpty(t, aux)

	bcc := new(Account)
	bcc.decode(aux)

	c := new(Account) //.decode([]byte{128, 0, 1, 128})
	ff := c.encode(nil)
	require.NotEmpty(t, ff)

	_ = c
	require.EqualValues(t, acc.Nonce, bcc.Nonce)
	require.True(t, acc.Balance.Eq(&bcc.Balance))
	require.EqualValues(t, acc.CodeHash, bcc.CodeHash)
}

func Test_BinPatriciaTrie_UniqueRepresentation(t *testing.T) {
	trie := NewBinaryPatriciaTrie()
	trieBatch := NewBinaryPatriciaTrie()

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("01", 12).
		Balance("f1", 120000).
		Nonce("aa", 152512).
		Balance("9a", 100000).
		Balance("e8", 200000).
		Balance("a2", 300000).
		Balance("f0", 400000).
		Balance("af", 500000).
		Balance("33", 600000).
		Nonce("aa", 184).
		Build()

	for i := 0; i < len(updates); i++ {
		_, err := trie.ProcessUpdates(plainKeys[i:i+1], hashedKeys[i:i+1], updates[i:i+1])
		require.NoError(t, err)
	}

	trieBatch.ProcessUpdates(plainKeys, hashedKeys, updates)

	hash, _ := trie.RootHash()
	require.Len(t, hash, 32)

	batchHash, _ := trieBatch.RootHash()
	require.EqualValues(t, hash, batchHash)

	for i, hkey := range hashedKeys {
		buf, ok := trie.Get(hkey)
		require.Truef(t, ok, "key %x should be present, but not found", plainKeys[i])
		buf2, ok := trieBatch.Get(hkey)
		require.True(t, ok)
		require.EqualValues(t, buf2, buf)
	}
}
