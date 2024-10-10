package args

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Args []string

func AsArgs(args interface{}) (Args, error) {

	argsValue := reflect.ValueOf(args)

	if argsValue.Kind() == reflect.Ptr {
		argsValue = argsValue.Elem()
	}

	if argsValue.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Args type must be struct or struc pointer, got %T", args)
	}

	return gatherArgs(argsValue, func(v reflect.Value, field reflect.StructField) (string, error) {
		tag := field.Tag.Get("arg")

		if tag == "-" {
			return "", nil
		}

		// only process public fields (reflection won't return values of unsafe fields without unsafe operations)
		if r, _ := utf8.DecodeRuneInString(field.Name); !(unicode.IsLetter(r) && unicode.IsUpper(r)) {
			return "", nil
		}

		var key string
		var positional bool

		for _, key = range strings.Split(tag, ",") {
			if key == "" {
				continue
			}

			key = strings.TrimLeft(key, " ")

			if pos := strings.Index(key, ":"); pos != -1 {
				key = key[:pos]
			}

			switch {
			case strings.HasPrefix(key, "---"):
				return "", fmt.Errorf("%s.%s: too many hyphens", v.Type().Name(), field.Name)
			case strings.HasPrefix(key, "--"):

			case strings.HasPrefix(key, "-"):
				if len(key) != 2 {
					return "", fmt.Errorf("%s.%s: short arguments must be one character only", v.Type().Name(), field.Name)
				}
			case key == "positional":
				key = ""
				positional = true
			default:
				return "", fmt.Errorf("unrecognized tag '%s' on field %s", key, tag)
			}
		}

		if len(key) == 0 && !positional {
			key = "--" + strings.ToLower(field.Name)
		}

		var value string

		switch fv := v.FieldByIndex(field.Index); fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if fv.Int() == 0 {
				break
			}
			fallthrough
		default:
			value = fmt.Sprintf("%v", fv.Interface())
		}

		flagValue, isFlag := field.Tag.Lookup("flag")

		if isFlag {
			if value != "true" {
				if flagValue == "true" {
					value = flagValue
				}
			}
		}

		if len(value) == 0 {
			if defaultString, hasDefault := field.Tag.Lookup("default"); hasDefault {
				value = defaultString
			}

			if len(value) == 0 {
				return "", nil
			}
		}

		if len(key) == 0 {
			return value, nil
		}

		if isFlag {
			if value == "true" {
				return key, nil
			}

			return "", nil
		}

		if len(value) == 0 {
			return key, nil
		}

		return fmt.Sprintf("%s=%s", key, value), nil
	})
}

func gatherArgs(v reflect.Value, visit func(v reflect.Value, field reflect.StructField) (string, error)) (args Args, err error) {
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)

		var gathered Args

		fieldType := field.Type

		if fieldType.Kind() == reflect.Ptr {
			fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct {
			gathered, err = gatherArgs(v.FieldByIndex(field.Index), visit)
		} else {
			var value string

			if value, err = visit(v, field); len(value) > 0 {
				gathered = Args{value}
			}
		}

		if err != nil {
			return nil, err
		}

		args = append(args, gathered...)
	}

	return args, nil
}
