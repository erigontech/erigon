package commitment

// Temporary exports for cross-package equivalence tests in nibbles/.
// Delete this file when old helpers are removed (Task 7).

// UncompactNibblesExported wraps unexported uncompactNibbles for the
// equivalence tripwire test in nibbles/equivalence_test.go.
func UncompactNibblesExported(key []byte) []byte {
	return uncompactNibbles(key)
}
