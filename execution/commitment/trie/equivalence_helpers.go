package trie

// Temporary exports for cross-package equivalence tests in nibbles/.
// Delete this file when old helpers are removed (Task 7).

// KeybytesToHexExported wraps unexported keybytesToHex for the
// equivalence tripwire test in nibbles/equivalence_test.go.
func KeybytesToHexExported(str []byte) []byte {
	return keybytesToHex(str)
}

// HexToKeybytesExported wraps unexported hexToKeybytes for the
// equivalence tripwire test in nibbles/equivalence_test.go.
func HexToKeybytesExported(hex []byte) []byte {
	return hexToKeybytes(hex)
}
