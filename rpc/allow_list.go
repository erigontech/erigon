package rpc

import "encoding/json"

type AllowList map[string]struct{}

func (a *AllowList) UnmarshalJSON(data []byte) error {
	var keys []string
	err := json.Unmarshal(data, &keys)
	if err != nil {
		return err
	}

	realA := make(map[string]struct{})

	for _, k := range keys {
		realA[k] = struct{}{}
	}

	*a = realA

	return nil
}

// MarshalJSON returns *m as the JSON encoding of
func (a *AllowList) MarshalJSON() ([]byte, error) {
	var realA map[string]struct{} = *a
	keys := make([]string, len(realA))
	i := 0
	for key := range realA {
		keys[i] = key
		i++
	}
	return json.Marshal(keys)
}

type ForbiddenList map[string]struct{}

func newForbiddenList() ForbiddenList {
	return ForbiddenList{}
}
