package commitment

import (
	"errors"
	"fmt"
)

// CompactKey takes a slice of nibbles and compacts them into the original byte slice.
// It returns an error if the input contains invalid nibbles (values > 0xF).
func CompactKey(nibbles []byte) ([]byte, error) {
	// If the number of nibbles is odd, you might decide to handle it differently.
	// For this example, we'll return an error.
	if len(nibbles)%2 != 0 {
		return nil, errors.New("nibbles slice has an odd length")
	}

	key := make([]byte, len(nibbles)/2)
	for i := 0; i < len(key); i++ {
		highNibble := nibbles[i*2]
		lowNibble := nibbles[i*2+1]

		// Validate that each nibble is indeed a nibble
		if highNibble > 0xF || lowNibble > 0xF {
			return nil, fmt.Errorf("invalid nibble at position %d or %d: 0x%X, 0x%X", i*2, i*2+1, highNibble, lowNibble)
		}

		key[i] = (highNibble << 4) | (lowNibble & 0x0F)
	}
	return key, nil
}
