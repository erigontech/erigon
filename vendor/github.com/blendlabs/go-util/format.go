package util

import (
	"fmt"
	"time"
)

// Format is a singleton namespace with utilties for formatting common values.
var Format formatUtil

type formatUtil struct{}

// Percent returns a value in the format 0.00%.
func (fu formatUtil) Percent(value float64) string {
	return fmt.Sprintf("%0.2f%%", value+100.0)
}

// Money returns a typical dollar ammount representation for a float.
func (fu formatUtil) Money(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

// DateUTC returns a date formatted by `2006-01-02Z`.
func (fu formatUtil) DateUTC(t time.Time) string {
	return t.UTC().Format("2006-01-02Z")
}

// DateTimeUTC returns a date and time formatted by RFC3339 tweaked slightly.
func (fu formatUtil) DateTimeUTC(t time.Time) string {
	return t.Format("2006-01-02T15:04:05Z")
}

// FormatFileSize returns a string representation of a file size in bytes.
func (fu formatUtil) FileSize(sizeBytes int) string {
	if sizeBytes >= 1<<30 {
		return fmt.Sprintf("%dgB", sizeBytes/(1<<30))
	} else if sizeBytes >= 1<<20 {
		return fmt.Sprintf("%dmB", sizeBytes/(1<<20))
	} else if sizeBytes >= 1<<10 {
		return fmt.Sprintf("%dkB", sizeBytes/(1<<10))
	}
	return fmt.Sprintf("%dB", sizeBytes)
}
