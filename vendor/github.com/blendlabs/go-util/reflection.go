package util

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/blendlabs/go-exception"
)

var (
	// Reflection is a namespace for reflection utilities.
	Reflection = reflectionUtil{}
)

// Patchable describes an object that can be patched with raw values.
type Patchable interface {
	Patch(values map[string]interface{}) error
}

type reflectionUtil struct{}

// FollowValuePointer derefs a reflectValue until it isn't a pointer, but will preseve it's nilness.
func (ru reflectionUtil) FollowValuePointer(v reflect.Value) interface{} {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return nil
	}

	val := v
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return val.Interface()
}

// FollowType derefs a type until it isn't a pointer or an interface.
func (ru reflectionUtil) FollowType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface {
		t = t.Elem()
	}
	return t
}

// FollowValue derefs a value until it isn't a pointer or an interface.
func (ru reflectionUtil) FollowValue(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

// ReflectValue returns the integral reflect.Value for an object.
func (ru reflectionUtil) ReflectValue(obj interface{}) reflect.Value {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

// ReflectType returns the integral type for an object.
func (ru reflectionUtil) ReflectType(obj interface{}) reflect.Type {
	t := reflect.TypeOf(obj)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}

// MakeNew returns a new instance of a reflect.Type.
func (ru reflectionUtil) MakeNew(t reflect.Type) interface{} {
	return reflect.New(t).Interface()
}

// MakeSliceOfType returns a new slice of a given reflect.Type.
func (ru reflectionUtil) MakeSliceOfType(t reflect.Type) interface{} {
	return reflect.New(reflect.SliceOf(t)).Interface()
}

// TypeName returns the string type name for an object's integral type.
func (ru reflectionUtil) TypeName(obj interface{}) string {
	return ru.ReflectType(obj).Name()
}

// GetValueByName returns a value for a given struct field by name.
func (ru reflectionUtil) GetValueByName(target interface{}, fieldName string) interface{} {
	targetValue := ru.ReflectValue(target)
	field := targetValue.FieldByName(fieldName)
	return field.Interface()
}

// GetFieldByNameOrJSONTag returns a value for a given struct field by name or by json tag name.
func (ru reflectionUtil) GetFieldByNameOrJSONTag(targetValue reflect.Type, fieldName string) *reflect.StructField {
	for index := 0; index < targetValue.NumField(); index++ {
		field := targetValue.Field(index)

		if field.Name == fieldName {
			return &field
		}
		tag := field.Tag
		jsonTag := tag.Get("json")
		if String.CaseInsensitiveEquals(jsonTag, fieldName) {
			return &field
		}
	}

	return nil
}

func (ru reflectionUtil) SetValueByName(target interface{}, fieldName string, fieldValue interface{}) error {
	targetValue := ru.ReflectValue(target)
	targetType := ru.ReflectType(target)
	return ru.SetValueByNameFromType(target, targetType, targetValue, fieldName, fieldValue)
}

// SetValueByName sets a value on an object by its field name.
func (ru reflectionUtil) SetValueByNameFromType(obj interface{}, targetType reflect.Type, targetValue reflect.Value, fieldName string, fieldValue interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = exception.Newf("panic setting value by name").WithMessagef("field: %s panic: %v", fieldName, r)
		}
	}()

	relevantField := ru.GetFieldByNameOrJSONTag(targetType, fieldName)
	if relevantField == nil {
		err = exception.New("unknown field").WithMessagef("%s `%s`", targetType.Name(), fieldName)
		return
	}

	field := targetValue.FieldByName(relevantField.Name)
	if !field.CanSet() {
		err = exception.New("cannot set field").WithMessagef("%s `%s`", targetType.Name(), fieldName)
		return
	}

	fieldType := field.Type()
	value := ru.ReflectValue(fieldValue)
	valueType := value.Type()
	if !value.IsValid() {
		err = exception.New("invalid value").WithMessagef("%s `%s`", targetType.Name(), fieldName)
		return
	}

	assigned, assignErr := ru.tryAssignment(fieldType, valueType, field, value)
	if assignErr != nil {
		err = assignErr
		return
	}
	if !assigned {
		err = exception.New("cannot set field")
		return
	}
	return
}

func (ru reflectionUtil) tryAssignment(fieldType, valueType reflect.Type, field, value reflect.Value) (assigned bool, err error) {
	if valueType.AssignableTo(fieldType) {
		field.Set(value)
		assigned = true
		return
	}

	if valueType.ConvertibleTo(fieldType) {
		convertedValue := value.Convert(fieldType)
		if convertedValue.Type().AssignableTo(fieldType) {
			field.Set(convertedValue)
			assigned = true
			return
		}
	}

	if fieldType.Kind() == reflect.Ptr {
		if valueType.AssignableTo(fieldType.Elem()) {
			elem := reflect.New(fieldType.Elem())
			elem.Elem().Set(value)
			field.Set(elem)
			assigned = true
			return
		} else if valueType.ConvertibleTo(fieldType.Elem()) {
			elem := reflect.New(fieldType.Elem())
			elem.Elem().Set(value.Convert(fieldType.Elem()))
			field.Set(elem)
			assigned = true
			return
		}
	}

	return
}

// PatchObject updates an object based on a map of field names to values.
func (ru reflectionUtil) PatchObject(obj interface{}, patchValues map[string]interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = exception.Newf("%v", r)
		}
	}()

	if patchable, isPatchable := obj.(Patchable); isPatchable {
		return patchable.Patch(patchValues)
	}

	targetValue := ru.ReflectValue(obj)
	targetType := targetValue.Type()

	for key, value := range patchValues {
		err = ru.SetValueByNameFromType(obj, targetType, targetValue, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// KeyValuePairOfString is a pair of string values.
type KeyValuePairOfString struct {
	Key, Value string
}

// DecomposeToPostData dumps an object to a slice of key value tuples representing field name as form value and string value of field.
func (ru reflectionUtil) DecomposeToPostData(object interface{}) []KeyValuePairOfString {
	kvps := []KeyValuePairOfString{}

	objType := ru.ReflectType(object)
	objValue := ru.ReflectValue(object)

	numberOfFields := objType.NumField()
	for index := 0; index < numberOfFields; index++ {
		field := objType.Field(index)
		valueField := objValue.Field(index)

		kvp := KeyValuePairOfString{}

		if !field.Anonymous {
			tag := field.Tag.Get("json")
			if len(tag) != 0 {
				if strings.Contains(tag, ",") {
					parts := strings.Split(tag, ",")
					kvp.Key = parts[0]
				} else {
					kvp.Key = tag
				}
			} else {
				kvp.Key = field.Name
			}

			if field.Type.Kind() == reflect.Slice {
				//do something special
				for subIndex := 0; subIndex < valueField.Len(); subIndex++ {
					itemAtIndex := valueField.Index(subIndex).Interface()
					for _, prop := range ru.DecomposeToPostData(itemAtIndex) {
						if len(prop.Value) != 0 { //this is a gutcheck, it shouldn't be needed
							ikvp := KeyValuePairOfString{}
							ikvp.Key = fmt.Sprintf("%s[%d].%s", kvp.Key, subIndex, prop.Key)
							ikvp.Value = prop.Value
							kvps = append(kvps, ikvp)
						}
					}
				}
			} else {
				value := ru.FollowValuePointer(valueField)
				if value != nil {
					kvp.Value = fmt.Sprintf("%v", value)
					if len(kvp.Value) != 0 {
						kvps = append(kvps, kvp)
					}
				}
			}
		}
	}

	return kvps
}

// DecomposeToPostDataAsJSON returns an array of KeyValuePairOfString for an object.
func (ru reflectionUtil) DecomposeToPostDataAsJSON(object interface{}) []KeyValuePairOfString {
	kvps := []KeyValuePairOfString{}

	objType := ru.ReflectType(object)
	objValue := ru.ReflectValue(object)

	numberOfFields := objType.NumField()
	for index := 0; index < numberOfFields; index++ {
		field := objType.Field(index)
		valueField := objValue.Field(index)

		kvp := KeyValuePairOfString{}

		if !field.Anonymous {
			tag := field.Tag.Get("json")
			if len(tag) != 0 {
				if strings.Contains(tag, ",") {
					parts := strings.Split(tag, ",")
					kvp.Key = parts[0]
				} else {
					kvp.Key = tag
				}
			} else {
				kvp.Key = field.Name
			}

			valueDereferenced := ru.FollowValue(valueField)
			value := ru.FollowValuePointer(valueField)
			if value != nil {
				if valueDereferenced.Kind() == reflect.Slice || valueDereferenced.Kind() == reflect.Map {
					kvp.Value = JSON.Serialize(value)
				} else {
					kvp.Value = fmt.Sprintf("%v", value)
				}
			}

			if len(kvp.Value) != 0 {
				kvps = append(kvps, kvp)
			}
		}
	}

	return kvps
}

// Decompose is a *very* inefficient way to turn an object into a map string => interface.
func (ru reflectionUtil) Decompose(object interface{}) map[string]interface{} {
	var output map[string]interface{}
	JSON.Deserialize(&output, JSON.Serialize(object))
	return output
}

// checks if a value is a zero value or its types default value
func (ru reflectionUtil) IsZero(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < v.Len(); i++ {
			z = z && ru.IsZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		for i := 0; i < v.NumField(); i++ {
			z = z && ru.IsZero(v.Field(i))
		}
		return z
	}
	// Compare other types directly:
	z := reflect.Zero(v.Type())
	return v.Interface() == z.Interface()
}

// IsExported returns if a field is exported given its name and capitalization.
func (ru reflectionUtil) IsExported(fieldName string) bool {
	return fieldName != "" && strings.ToUpper(fieldName)[0] == fieldName[0]
}

// CoalesceFields merges non-zero fields into destination fields marked with the `coalesce:...` struct field tag.
func (ru reflectionUtil) CoalesceFields(object interface{}) {
	objectValue := ru.ReflectValue(object)
	objectType := ru.ReflectType(object)
	if objectType.Kind() == reflect.Struct {
		numberOfFields := objectValue.NumField()
		for index := 0; index < numberOfFields; index++ {
			field := objectType.Field(index)
			fieldValue := objectValue.Field(index)
			// only alter the field if it is exported (uppercase variable name) and is not already a non-zero value
			if ru.IsExported(field.Name) && ru.IsZero(fieldValue) {
				alternateFieldNames := strings.Split(field.Tag.Get("coalesce"), ",")

				// find the first non-zero value in the list of backup values
				for j := 0; j < len(alternateFieldNames); j++ {
					alternateFieldName := alternateFieldNames[j]
					alternateValue := objectValue.FieldByName(alternateFieldName)
					// will panic if trying to set a non-exported value or a zero value, so ignore those
					if ru.IsExported(alternateFieldName) && !ru.IsZero(alternateValue) {
						fieldValue.Set(alternateValue)
						break
					}
				}
			}
			// recurse, in case nested values of this field need to be set as well
			if ru.IsExported(field.Name) && !ru.IsZero(fieldValue) {
				ru.CoalesceFields(fieldValue.Addr().Interface())
			}
		}
	} else if objectType.Kind() == reflect.Array || objectType.Kind() == reflect.Slice {
		arrayLength := objectValue.Len()
		for i := 0; i < arrayLength; i++ {
			ru.CoalesceFields(objectValue.Index(i).Addr().Interface())
		}
	}
}
