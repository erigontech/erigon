package util

import "time"

// Coalesce is a set of utilities for coalesced values.
var Coalesce coalesceUtil

type coalesceUtil struct{}

// String returns a coalesced value.
func (cu coalesceUtil) String(value, defaultValue string, inheritedValues ...string) string {
	if len(value) > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Bool returns a coalesced value.
func (cu coalesceUtil) Bool(value *bool, defaultValue bool, inheritedValues ...bool) bool {
	if value != nil {
		return *value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Int returns a coalesced value.
func (cu coalesceUtil) Int(value, defaultValue int, inheritedValues ...int) int {
	if value > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Int32 returns a coalesced value.
func (cu coalesceUtil) Int32(value, defaultValue int32, inheritedValues ...int32) int32 {
	if value > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Int returns a coalesced value.
func (cu coalesceUtil) Int64(value, defaultValue int64, inheritedValues ...int64) int64 {
	if value > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Float32 returns a coalesced value.
func (cu coalesceUtil) Float32(value, defaultValue float32, inheritedValues ...float32) float32 {
	if value > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Float64 returns a coalesced value.
func (cu coalesceUtil) Float64(value, defaultValue float64, inheritedValues ...float64) float64 {
	if value > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Duration returns a coalesced value.
func (cu coalesceUtil) Duration(value, defaultValue time.Duration, inheritedValues ...time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Time returns a coalesced value.
func (cu coalesceUtil) Time(value, defaultValue time.Time, inheritedValues ...time.Time) time.Time {
	if !value.IsZero() {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Strings returns a coalesced value.
func (cu coalesceUtil) Strings(value, defaultValue []string, inheritedValues ...[]string) []string {
	if len(value) > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}

// Bytes returns a coalesced value.
func (cu coalesceUtil) Bytes(value, defaultValue []byte, inheritedValues ...[]byte) []byte {
	if len(value) > 0 {
		return value
	}
	if len(inheritedValues) > 0 {
		return inheritedValues[0]
	}
	return defaultValue
}
