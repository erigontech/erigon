package linq

import (
	"reflect"
	"strconv"
	"time"

	"github.com/blendlabs/go-util"
)

// Predicate is a function that returns a boolean for an input.
type Predicate func(item interface{}) bool

// PredicateOfByte is a function that returns a boolean for an input.
type PredicateOfByte func(item byte) bool

// PredicateOfRune is a function that returns a boolean for an input.
type PredicateOfRune func(item rune) bool

// PredicateOfInt is a function that returns a boolean for an input.
type PredicateOfInt func(item int) bool

// PredicateOfInt64 is a function that returns a boolean for an input.
type PredicateOfInt64 func(item int64) bool

// PredicateOfFloat is a function that returns a boolean for an input.
type PredicateOfFloat func(item float64) bool

// PredicateOfString is a function that returns a boolean for an input.
type PredicateOfString func(item string) bool

// PredicateOfTime is a function that returns a boolean for an input.
type PredicateOfTime func(item time.Time) bool

// PredicateOfDuration is a function that returns a boolean for an input.
type PredicateOfDuration func(item time.Duration) bool

// MapAction is an action that returns a value for an input value, a.k.a. that maps the two.
type MapAction func(item interface{}) interface{}

// MapActionOfByte is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfByte func(item byte) byte

// MapActionOfRune is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfRune func(item rune) rune

// MapActionOfInt is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfInt func(item int) int

// MapActionOfInt64 is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfInt64 func(item int64) int64

// MapActionOfFloat is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfFloat func(item float64) float64

// MapActionOfString is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfString func(item string) string

// MapActionOfTime is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfTime func(item time.Time) time.Time

// MapActionOfDuration is an action that returns a value for an input value, a.k.a. that maps the two.
type MapActionOfDuration func(item time.Duration) time.Time

// ReturnsTrue is a pre-built predicate.
func ReturnsTrue() Predicate {
	return func(_ interface{}) bool {
		return true
	}
}

// ReturnsTrueOfByte is a pre-built predicate.
func ReturnsTrueOfByte() PredicateOfByte {
	return func(_ byte) bool {
		return true
	}
}

// ReturnsTrueOfRune is a pre-built predicate.
func ReturnsTrueOfRune() PredicateOfRune {
	return func(_ rune) bool {
		return true
	}
}

// ReturnsTrueOfInt is a pre-built predicate.
func ReturnsTrueOfInt() PredicateOfInt {
	return func(_ int) bool {
		return true
	}
}

// ReturnsTrueOfFloat is a pre-built predicate.
func ReturnsTrueOfFloat() PredicateOfFloat {
	return func(_ float64) bool {
		return true
	}
}

// ReturnsTrueOfString is a pre-built predicate.
func ReturnsTrueOfString() PredicateOfString {
	return func(_ string) bool {
		return true
	}
}

// ReturnsTrueOfTime is a pre-built predicate.
func ReturnsTrueOfTime() PredicateOfTime {
	return func(_ time.Time) bool {
		return true
	}
}

// ReturnsTrueOfDuration is a pre-built predicate.
func ReturnsTrueOfDuration() PredicateOfDuration {
	return func(_ time.Duration) bool {
		return true
	}
}

// ReturnsFalse is a pre-built predicate.
func ReturnsFalse() Predicate {
	return func(_ interface{}) bool {
		return false
	}
}

// ReturnsFalseOfByte is a pre-built predicate.
func ReturnsFalseOfByte() PredicateOfByte {
	return func(_ byte) bool {
		return false
	}
}

// ReturnsFalseOfRune is a pre-built predicate.
func ReturnsFalseOfRune() PredicateOfRune {
	return func(_ rune) bool {
		return false
	}
}

// ReturnsFalseOfInt is a pre-built predicate.
func ReturnsFalseOfInt() PredicateOfInt {
	return func(_ int) bool {
		return false
	}
}

// ReturnsFalseOfFloat is a pre-built predicate.
func ReturnsFalseOfFloat() PredicateOfFloat {
	return func(_ float64) bool {
		return false
	}
}

// ReturnsFalseOfString is a pre-built predicate.
func ReturnsFalseOfString() PredicateOfString {
	return func(_ string) bool {
		return false
	}
}

// ReturnsFalseOfTime is a pre-built predicate.
func ReturnsFalseOfTime() PredicateOfTime {
	return func(_ time.Time) bool {
		return false
	}
}

// ReturnsFalseOfDuration is a pre-built predicate.
func ReturnsFalseOfDuration() PredicateOfDuration {
	return func(_ time.Duration) bool {
		return false
	}
}

// DeepEqual is a pre-built predicate that compares shouldBe to input objects.
func DeepEqual(shouldBe interface{}) Predicate {
	return func(value interface{}) bool {
		return reflect.DeepEqual(shouldBe, value)
	}
}

// EqualsOfByte is a pre-built predicate that compares shouldBe to input objects.
func EqualsOfByte(shouldBe byte) PredicateOfByte {
	return func(value byte) bool {
		return shouldBe == value
	}
}

// EqualsOfRune is a pre-built predicate that compares shouldBe to input objects.
func EqualsOfRune(shouldBe rune) PredicateOfRune {
	return func(value rune) bool {
		return shouldBe == value
	}
}

// EqualsOfInt is a pre-built predicate that compares shouldBe to input objects.
func EqualsOfInt(shouldBe int) PredicateOfInt {
	return func(value int) bool {
		return shouldBe == value
	}
}

// EqualsOfFloat is a pre-built predicate that compares shouldBe to input objects.
func EqualsOfFloat(shouldBe float64) PredicateOfFloat {
	return func(value float64) bool {
		return shouldBe == value
	}
}

// EqualsOfString is a pre-built predicate that compares shouldBe to input objects.
func EqualsOfString(shouldBe string) PredicateOfString {
	return func(value string) bool {
		return shouldBe == value
	}
}

// EqualsOfStringCaseInsenitive is a pre-built predicate that compares shouldBe to input objects.
func EqualsOfStringCaseInsenitive(shouldBe string) PredicateOfString {
	return func(value string) bool {
		return util.String.CaseInsensitiveEquals(shouldBe, value)
	}
}

// StringToInt is a pre-built map function.
func StringToInt(item interface{}) interface{} {
	if itemAsString, isString := item.(string); isString {
		if intValue, intValueErr := strconv.Atoi(itemAsString); intValueErr == nil {
			return intValue
		}
	}

	return nil
}

// StringToFloat is a pre-built map function.
func StringToFloat(item interface{}) interface{} {
	if itemAsString, isString := item.(string); isString {
		if floatValue, floatValueErr := strconv.ParseFloat(itemAsString, 64); floatValueErr == nil {
			return floatValue
		}
	}

	return nil
}

type stringable interface {
	String() string
}

// ValueToString is a pre-built map function.
func ValueToString(value interface{}) interface{} {
	if typed, isTyped := value.(stringable); isTyped {
		return typed.String()
	}
	return ""
}

// Any returns true if the predicate holds for any object in the collection.
func Any(target interface{}, predicate Predicate) bool {
	t := reflect.TypeOf(target)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		return false
	}

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface()
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfByte returns true if the predicate holds for any object in the collection.
func AnyOfByte(target []byte, predicate PredicateOfByte) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(byte)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfRune returns true if the predicate holds for any object in the collection.
func AnyOfRune(target []rune, predicate PredicateOfRune) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(rune)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfInt returns true if the predicate holds for any object in the collection.
func AnyOfInt(target []int, predicate PredicateOfInt) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(int)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfFloat returns true if the predicate holds for any object in the collection.
func AnyOfFloat(target []float64, predicate PredicateOfFloat) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(float64)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfString returns true if the predicate holds for any object in the collection.
func AnyOfString(target []string, predicate PredicateOfString) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(string)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfTime returns true if the predicate holds for any object in the collection.
func AnyOfTime(target []time.Time, predicate PredicateOfTime) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(time.Time)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// AnyOfDuration returns true if the predicate holds for any object in the collection.
func AnyOfDuration(target []time.Duration, predicate PredicateOfDuration) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(time.Duration)
		if predicate == nil || predicate(obj) {
			return true
		}
	}
	return false
}

// All returns true if the predicate holds for all objects in the collection.
func All(target interface{}, predicate Predicate) bool {
	t := reflect.TypeOf(target)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		return false
	}

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface()
		if !predicate(obj) {
			return false
		}
	}
	return true
}

// AllOfInt returns true if the predicate holds for all objects in the collection.
func AllOfInt(target []int, predicate PredicateOfInt) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(int)
		if !predicate(obj) {
			return false
		}
	}
	return true
}

// AllOfFloat returns true if the predicate holds for all objects in the collection.
func AllOfFloat(target []float64, predicate PredicateOfFloat) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(float64)
		if !predicate(obj) {
			return false
		}
	}
	return true
}

// AllOfString returns true if the predicate holds for all objects in the collection.
func AllOfString(target []string, predicate PredicateOfString) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(string)
		if !predicate(obj) {
			return false
		}
	}
	return true
}

// AllOfTime returns true if the predicate holds for all objects in the collection.
func AllOfTime(target []time.Time, predicate PredicateOfTime) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(time.Time)
		if !predicate(obj) {
			return false
		}
	}
	return true
}

// AllOfDuration returns true if the predicate holds for all objects in the collection.
func AllOfDuration(target []time.Duration, predicate PredicateOfDuration) bool {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(time.Duration)
		if !predicate(obj) {
			return false
		}
	}
	return true
}

// First returns the first object that satisfies a predicate.
func First(target interface{}, predicate Predicate) interface{} {
	t := reflect.TypeOf(target)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		return false
	}

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface()
		if predicate == nil || predicate(obj) {
			return obj
		}
	}
	return nil
}

// FirstOfByte returns the first object that satisfies a predicate.
func FirstOfByte(target []byte, predicate PredicateOfByte) *byte {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(byte)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// FirstOfInt returns the first object that satisfies a predicate.
func FirstOfInt(target []int, predicate PredicateOfInt) *int {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(int)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// FirstOfFloat returns the first object that satisfies a predicate.
func FirstOfFloat(target []float64, predicate PredicateOfFloat) *float64 {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(float64)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// FirstOfString returns the first object that satisfies a predicate.
func FirstOfString(target []string, predicate PredicateOfString) *string {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(string)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// FirstOfTime returns the first object that satisfies a predicate.
func FirstOfTime(target []time.Time, predicate PredicateOfTime) *time.Time {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(time.Time)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// FirstOfDuration returns the first object that satisfies a predicate.
func FirstOfDuration(target []time.Duration, predicate PredicateOfDuration) *time.Duration {
	v := reflect.ValueOf(target)

	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface().(time.Duration)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// Last returns the last object that satisfies a predicate.
func Last(target interface{}, predicate Predicate) interface{} {
	t := reflect.TypeOf(target)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		return false
	}

	for x := v.Len() - 1; x > 0; x-- {
		obj := v.Index(x).Interface()
		if predicate == nil || predicate(obj) {
			return obj
		}
	}

	return nil
}

// LastOfInt returns the last object that satisfies a predicate.
func LastOfInt(target []int, predicate PredicateOfInt) *int {
	v := reflect.ValueOf(target)

	for x := v.Len() - 1; x > 0; x-- {
		obj := v.Index(x).Interface().(int)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// LastOfFloat returns the last object that satisfies a predicate.
func LastOfFloat(target []float64, predicate PredicateOfFloat) *float64 {
	v := reflect.ValueOf(target)

	for x := v.Len() - 1; x > 0; x-- {
		obj := v.Index(x).Interface().(float64)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// LastOfString returns the last object that satisfies a predicate.
func LastOfString(target []string, predicate PredicateOfString) *string {
	v := reflect.ValueOf(target)

	for x := v.Len() - 1; x > 0; x-- {
		obj := v.Index(x).Interface().(string)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// LastOfTime returns the last object that satisfies a predicate.
func LastOfTime(target []time.Time, predicate PredicateOfTime) *time.Time {
	v := reflect.ValueOf(target)

	for x := v.Len() - 1; x > 0; x-- {
		obj := v.Index(x).Interface().(time.Time)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// LastOfDuration returns the last object that satisfies a predicate.
func LastOfDuration(target []time.Duration, predicate PredicateOfDuration) *time.Duration {
	v := reflect.ValueOf(target)

	for x := v.Len() - 1; x > 0; x-- {
		obj := v.Index(x).Interface().(time.Duration)
		if predicate == nil || predicate(obj) {
			return &obj
		}
	}
	return nil
}

// Filter applies a predicate to a collection.
func Filter(target interface{}, predicate Predicate) []interface{} {
	t := reflect.TypeOf(target)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		panic("cannot filter non-slice.")
	}

	values := []interface{}{}
	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface()
		if predicate(obj) {
			values = append(values, obj)
		}
	}
	return values
}

// Select maps the values of the target collection to the mapFn.
func Select(target interface{}, mapFn MapAction) []interface{} {
	t := reflect.TypeOf(target)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		panic("cannot map non-slice.")
	}

	values := []interface{}{}
	for x := 0; x < v.Len(); x++ {
		obj := v.Index(x).Interface()
		values = append(values, mapFn(obj))
	}
	return values
}
