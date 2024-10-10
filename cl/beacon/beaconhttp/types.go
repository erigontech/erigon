package beaconhttp

import (
	"encoding/json"
	"strconv"
)

type IntStr int

func (i IntStr) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatInt(int64(i), 10))
}

func (i *IntStr) UnmarshalJSON(b []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		value, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*i = IntStr(value)
		return nil
	}

	// Fallback to number
	return json.Unmarshal(b, (*int)(i))
}
