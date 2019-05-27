package util

import "time"

// OptionalUInt8 returns a pointer to a value
func OptionalUInt8(value uint8) *uint8 {
	return &value
}

// OptionalUInt16 returns a pointer to a value
func OptionalUInt16(value uint16) *uint16 {
	return &value
}

// OptionalUInt returns a pointer to a value
func OptionalUInt(value uint) *uint {
	return &value
}

// OptionalUInt64 returns a pointer to a value
func OptionalUInt64(value uint64) *uint64 {
	return &value
}

// OptionalInt16 returns a pointer to a value
func OptionalInt16(value int16) *int16 {
	return &value
}

// OptionalInt returns a pointer to a value
func OptionalInt(value int) *int {
	return &value
}

// OptionalInt32 returns a pointer to a value
func OptionalInt32(value int32) *int32 {
	return &value
}

// OptionalInt64 returns a pointer to a value
func OptionalInt64(value int64) *int64 {
	return &value
}

// OptionalFloat32 returns a pointer to a value
func OptionalFloat32(value float32) *float32 {
	return &value
}

// OptionalFloat64 returns a pointer to a value
func OptionalFloat64(value float64) *float64 {
	return &value
}

// OptionalString returns a pointer to a value
func OptionalString(value string) *string {
	return &value
}

// OptionalBool returns a pointer to a value
func OptionalBool(value bool) *bool {
	return &value
}

// OptionalTime returns a pointer to a value
func OptionalTime(value time.Time) *time.Time {
	return &value
}

// OptionalDuration returns a pointer to a value
func OptionalDuration(value time.Duration) *time.Duration {
	return &value
}
