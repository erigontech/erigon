package main

import (
	"fmt"
	"regexp"
)

const (
	str_uint64  = "uint64"
	str_bigInt  = "big.Int"
	str_uint256 = "uint256.Int"
	str_address = "Address"
	str_hash    = "Hash"
)

func matchPointerType(typ string) (string, error) {

	if typ[0] != '*' {
		return "", fmt.Errorf("expected string starting with '*' (pointer type)")
	}

	typ = typ[1:]

	// fmt.Println(typ)

	var typs = []string{
		str_uint64,
		str_bigInt,
		str_uint256,
		str_address,
		str_hash,
	}

	for _, t := range typs {
		if matched, err := regexp.MatchString(t, typ); matched {
			return t, nil
		} else if err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf("no match found for: %s", typ)
}
