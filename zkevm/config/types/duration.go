package types

import "time"

// Duration is a wrapper type that parses time duration from text.
type Duration struct {
	time.Duration `validate:"required"`
}

// UnmarshalText unmarshalls time duration from text.
func (d *Duration) UnmarshalText(data []byte) error {
	duration, err := time.ParseDuration(string(data))
	if err != nil {
		return err
	}
	d.Duration = duration
	return nil
}

// NewDuration returns Duration wrapper
func NewDuration(duration time.Duration) Duration {
	return Duration{time.Duration(duration)}
}
